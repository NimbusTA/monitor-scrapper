"""This module contains an implementation of the ScalarMetricsExporter class which contains methods of getting from the
relay chain and parachain and exposing them to Prometheus."""
import logging
import sys
import threading
import time

from threading import Lock, Thread
from typing import Tuple, Union


from datetime import datetime
from substrateinterface import Keypair
from substrateinterface.exceptions import SubstrateRequestException, BlockNotFound

import utils

from contracts_reader import ContractsReader
from database_manager import DatabaseManager
from database_manager_token_price_collector import DatabaseManagerTokenPriceCollector
from decorators import reconnection_counter
from prometheus_metrics import metrics_exporter
from service_parameters import ServiceParameters


DAYS_IN_MONTH = 30
DAYS_IN_WEEK = 7
MSECS_IN_SECOND = 1_000
SECOND = 1

logger = logging.getLogger(__name__)


class ScalarMetricsExporter(Thread):
    """This class contains methods of getting scalar metrics from the relay chain and parachain and exposing them
    to Prometheus.
    """
    database_manager: DatabaseManager
    database_manager_token_price_collector: DatabaseManagerTokenPriceCollector or None
    service_params: ServiceParameters

    active_era_id_prev: int
    active_era_start_block_number: int
    block_number_para_prev: int
    block_number_relay_prev: int
    contracts_reader: ContractsReader
    initialized: bool
    ledger_addresses: set
    lock: Lock
    stashes: dict
    stop: bool

    def __init__(self,
                 database_manager: DatabaseManager,
                 database_manager_token_price_collector: DatabaseManagerTokenPriceCollector,
                 service_params: ServiceParameters):
        Thread.__init__(self)
        self.database_manager = database_manager
        self.database_manager_token_price_collector = database_manager_token_price_collector
        self.service_params = service_params

        self.active_era_id_prev = -1
        self.active_era_start_block_number = -1
        self.block_number_para_prev = -1
        self.block_number_relay_prev = -1
        self.contracts_reader = ContractsReader(service_params)
        self.initialized = False
        self.ledger_addresses = set()
        self.lock = threading.Lock()
        self.stashes = {}
        self.stop = False

    def run(self):
        while True:
            self._run()

    @reconnection_counter.reconnection_counter
    def _run(self):
        """ScalarMetricsExporter"""
        try:
            block_hash = self.service_params.substrate_para__scalar_metrics.get_chain_finalised_head()
            block_number = self.service_params.substrate_para__scalar_metrics.get_block_number(block_hash)
            if block_number > self.block_number_para_prev:
                self.handle_block_para(block_hash, block_number)

            block_hash = self.service_params.substrate_relay__scalar_metrics.get_chain_finalised_head()
            block_number = self.service_params.substrate_relay__scalar_metrics.get_block_number(block_hash)
            if block_number > self.block_number_relay_prev:
                self.handle_block_relay(block_hash, block_number)

            reconnection_counter.remove_thread(self._run.__doc__)
            self.initialized = True
            time.sleep(SECOND)
        except Exception as exc:
            exc_type = type(exc)
            if exc_type in utils.EXPECTED_NETWORK_EXCEPTIONS:
                logger.warning("An expected error occurred: %s - %s", type(exc), exc)
            else:
                logger.critical("An unexpected error occurred: %s - %s", type(exc), exc)
            metrics_exporter.alert_not_connected_scalar_metrics_exporter.set(int(True))
            self.service_params.substrate_relay__scalar_metrics = utils.restore_connection_to_relay_chain(
                timeout=self.service_params.timeout,
                ws_urls=self.service_params.ws_urls_relay,
                substrate=self.service_params.substrate_relay__scalar_metrics,
            )
            self.service_params.substrate_para__scalar_metrics, self.service_params.w3__scalar_metrics = \
                utils.restore_connection_to_parachain(
                    timeout=self.service_params.timeout,
                    substrate=self.service_params.substrate_para__scalar_metrics,
                    ws_urls=self.service_params.ws_urls_para,
                    w3=self.service_params.w3__scalar_metrics,
                )
            metrics_exporter.alert_not_connected_scalar_metrics_exporter.set(int(False))

    def handle_block_relay(self, block_hash: str, block_number: int):
        """Get and expose metrics from the relay chain."""
        with self.lock:
            if self.stop:
                sys.exit()

        logger.info("[relay] Current block: %s - %s", block_number, block_hash)

        active_era_id = self.service_params.substrate_relay__scalar_metrics.query('Staking', 'ActiveEra')['index'].value
        logger.info("[relay] The active era: %s", active_era_id)
        metrics_exporter.relay_chain_active_era_id.set(active_era_id)
        self.get_and_expose_next_era_start_time_in_relay(active_era_id, block_hash, block_number)
        self.get_and_expose_ledger_metrics(block_hash)
        self.calculate_and_expose_apr_per_month_and_week()
        self.calculate_inflation_rate(active_era_id)

        if self.service_params.payout_service_address:
            logger.info("[relay] Getting the balance of the payout service")
            balance = self.service_params.substrate_relay__scalar_metrics.query(
                module='System',
                storage_function='Account',
                params=[self.service_params.payout_service_address],
            ).value['data']['free']
            metrics_exporter.payout_service_balance.labels(self.service_params.payout_service_address).set(balance)
            logger.info("[relay] The payout service balance: %s", balance)

        self.block_number_relay_prev = block_number
        logger.info("[relay] Waiting for the next block")

    def handle_block_para(self, block_hash: str, block_number: int):
        """Get and expose metrics from the parachain."""
        with self.lock:
            if self.stop:
                sys.exit()

        logger.info("[para] Current block: %s - %s", block_number, block_hash)
        metrics_exporter.parachain_block_number.set(block_number)

        self.stashes = self.contracts_reader.get_stashes_and_ledgers(block_number)
        self.contracts_reader.get_and_expose_oracle_balances(block_number)
        self.contracts_reader.get_and_expose_next_era_start_time_in_contract(block_number)
        self.contracts_reader.get_and_expose_era_values_from_oracle_master(block_number)
        self.ledger_addresses = self.contracts_reader.get_ledger_addresses(block_number)
        self.contracts_reader.get_and_expose_ledger_metrics(block_number, self.ledger_addresses)
        self.contracts_reader.get_and_expose_controller_balance(block_number)
        self.contracts_reader.get_and_expose_withdrawal_metrics(block_number)

        token_price = self.get_token_price()
        total_supply = self.contracts_reader.get_and_expose_nimbus_metrics(block_number, token_price)
        self.database_manager.update_api_table({'total_supply': total_supply})

        self.block_number_para_prev = block_number
        logger.info("[para] Waiting for the next block")

    def calculate_and_expose_apr_per_month_and_week(self):
        """Calculate, save in the 'api' table and expose to Prometheus the APR per month and week."""
        logger.info("[relay] Calculating the APR per month and week")
        apr_per_month = self._calculate_apr(self.service_params.eras_per_day * DAYS_IN_MONTH)
        if apr_per_month:
            metrics_exporter.apr_per_month.set(apr_per_month)
            self.database_manager.update_api_table({'apr_per_month': apr_per_month})

        apr_per_week = self._calculate_apr(self.service_params.eras_per_day * DAYS_IN_WEEK)
        if apr_per_week:
            metrics_exporter.apr_per_month.set(apr_per_week)
            self.database_manager.update_api_table({'apr_per_week': apr_per_week})

    def _calculate_apr(self, time_range: int) -> Union[float, None]:
        """Calculate APR based on the given time range."""
        rewards_month = {}
        for ledger in self.ledger_addresses:
            ledger_rewards_month = self.database_manager.get_rewards(ledger, time_range)
            rewards_number_month = self.database_manager.get_rewards_number(ledger)
            if rewards_number_month:
                utils.remove_redundant_rewards_entries(ledger_rewards_month, rewards_number_month[0], time_range)
            if ledger_rewards_month:
                rewards_month[ledger] = ledger_rewards_month
        if rewards_month:
            apr_per_month = utils.calculate_apr(
                0, rewards_month, self.service_params.eras_per_day, self.service_params.apr_min, self.service_params.apr_max)
            metrics_exporter.apr_per_month.set(apr_per_month)
            return utils.calculate_apr(0, rewards_month, self.service_params.eras_per_day,
                                       self.service_params.apr_min, self.service_params.apr_max)

        return None

    def get_and_expose_next_era_start_time_in_relay(self, active_era_id: int, block_hash: str, block_number: int):
        """Get and expose the ext era start time to Prometheus."""
        logger.info("[relay] Getting the next era start time")
        era_duration_in_blocks = self.service_params.era_duration_in_blocks

        block_timestamp = 0
        block_extrinsics = self.service_params.substrate_relay__scalar_metrics.get_block(block_hash)['extrinsics']
        for extrinsic in block_extrinsics:
            if extrinsic['call']['call_module']['name'] == 'Timestamp':
                if extrinsic['call']['call_function']['name'] == 'set':
                    block_timestamp = extrinsic['call']['call_args'][0]['value'].value // MSECS_IN_SECOND
                    break
        if self.active_era_id_prev != active_era_id:
            self.active_era_start_block_number = self._get_era_first_block_number(active_era_id)
            self.active_era_id_prev = active_era_id
            logger.info("[relay] The active era has started at the block %s", self.active_era_start_block_number)

        next_era_start_time = (era_duration_in_blocks - (block_number - self.active_era_start_block_number)) * 6 \
            + block_timestamp
        metrics_exporter.relay_chain_next_era_start_time.set(next_era_start_time)

    def get_and_expose_ledger_metrics(self, block_hash: str):
        """Get and expose ledger metrics to prometheus."""
        staking_validators = self.service_params.substrate_relay__scalar_metrics.query(
            module='Session',
            storage_function='Validators',
            block_hash=block_hash,
        )
        validators = set(validator for validator in staking_validators.value)

        for stash_account, ledger in self.stashes.items():
            logger.info("[relay] Getting scalar metrics for the ledger %s", ledger)

            stash = Keypair(public_key=stash_account, ss58_format=self.service_params.ss58_format_para)
            controller = self.service_params.substrate_relay__scalar_metrics.query(
                module='Staking',
                storage_function='Bonded',
                params=[stash.ss58_address],
                block_hash=block_hash,
            )
            stash_balance = self.service_params.substrate_relay__scalar_metrics.query(
                module='System',
                storage_function='Account',
                params=[stash.ss58_address],
                block_hash=block_hash,
            ).value['data']['free']

            self.get_and_expose_data_unlocking(block_hash, stash)

            if controller.value is None:
                metrics_exporter.relay_chain_ledger_active_balance.labels(ledger).set(0)
                metrics_exporter.relay_chain_ledger_total_balance.labels(ledger).set(0)
                metrics_exporter.relay_chain_ledger_stash_balance.labels(ledger).set(stash_balance)
                metrics_exporter.relay_chain_ledger_stake_status.labels(ledger).set(3)
                metrics_exporter.relay_chain_ledger_validators_count.labels(ledger).set(0)
                continue

            controller = Keypair(ss58_address=controller.value)
            ledger_info = self.service_params.substrate_relay__scalar_metrics.query(
                module='Staking',
                storage_function='Ledger',
                params=[controller.ss58_address],
                block_hash=block_hash,
            )

            metrics_exporter.relay_chain_ledger_active_balance.labels(ledger).set(ledger_info.value['active'])
            metrics_exporter.relay_chain_ledger_total_balance.labels(ledger).set(ledger_info.value['total'])
            metrics_exporter.relay_chain_ledger_stash_balance.labels(ledger).set(stash_balance)

            stake_status = self.get_stake_status(stash.ss58_address, validators, block_hash)
            metrics_exporter.relay_chain_ledger_stake_status.labels(ledger).set(stake_status)

            validators_stash_keys = self.service_params.substrate_relay__scalar_metrics.query(
                module='Staking',
                storage_function='Nominators',
                params=[controller.ss58_address],
                block_hash=block_hash,
            )
            if validators_stash_keys.value is not None:
                validators_count = len(validators_stash_keys.value['targets'])
            else:
                validators_count = 0
            metrics_exporter.relay_chain_ledger_validators_count.labels(ledger).set(validators_count)

            logger.info("[relay] Scalar metrics for the ledger %s are gotten successfully", ledger)

    def get_and_expose_data_unlocking(self, block_hash: str, stash: Keypair):
        """Get and expose data unlocking for the given stash."""
        logger.info("[relay] Getting the unlocking data for the ledger %s", stash.ss58_address)
        ledger_data = self.service_params.substrate_relay__scalar_metrics.query(
            module='Staking',
            storage_function='Ledger',
            params=[stash.ss58_address],
            block_hash=block_hash,
        )

        earliest_era = 0
        total_unlocking = 0
        if ledger_data is not None and ledger_data.value is not None:
            data_unlocking = ledger_data.value['unlocking']
            for data in data_unlocking:
                earliest_era = data['era'] if data['era'] < earliest_era or earliest_era == 0 else earliest_era
                total_unlocking += data['value']

        metrics_exporter.relay_chain_ledger_total_unlocking_balance.labels(stash.ss58_address).set(total_unlocking)
        metrics_exporter.relay_chain_ledger_earliest_era_for_unlocking.labels(stash.ss58_address).set(earliest_era)

    def get_stake_status(self, stash: str, validators: set, block_hash: str) -> int:
        """ Get stash account status. Returns the status as an integer: 0 - Idle, 1 - Nominator, 2 - Validator."""
        logger.info("[relay] Getting the stake status: %s", stash)
        if self.service_params.substrate_relay__scalar_metrics.query(
            module='Staking',
            storage_function='Nominators',
            params=[stash],
            block_hash=block_hash,
        ).value is not None:
            return 1

        if stash in validators:
            return 2

        return 0

    def calculate_inflation_rate(self, active_era_id: int) -> Tuple[float, float]:
        """Calculate the inflation rate according to the Polkadot UI algorithm. Save the result in the 'api' table."""
        logger.info("[relay] Calculating the inflation rate and the staked fraction")
        total_staked = self.service_params.substrate_relay__scalar_metrics.query(
            module='Staking',
            storage_function='ErasTotalStake',
            params=[active_era_id],
        ).value
        metrics_exporter.relay_chain_total_staked_tokens.set(total_staked)

        num_auctions = self.service_params.substrate_relay__scalar_metrics.query('Auctions', 'AuctionCounter').value
        total_issuance = self.service_params.substrate_relay__scalar_metrics.query('Balances', 'TotalIssuance').value

        if total_staked == 0 or total_issuance == 0:
            staked_fraction = 0
        else:
            staked_fraction = total_staked / total_issuance
        logger.info("[relay] Staked fraction: %s", staked_fraction)

        ideal_stake = self.service_params.stake_target \
            - min(self.service_params.auction_max, num_auctions) * self.service_params.auction_adjust
        ideal_interest = self.service_params.max_inflation / ideal_stake
        min_inflation = self.service_params.min_inflation
        if staked_fraction <= ideal_stake:
            inflation_rate = min_inflation + staked_fraction * (ideal_interest - min_inflation / ideal_stake)
        else:
            power_of_two = 2 ** ((ideal_stake - staked_fraction) / self.service_params.falloff)
            inflation_rate = min_inflation + (ideal_interest * ideal_stake - min_inflation) * power_of_two
        logger.info("[relay] Inflation rate: %s", inflation_rate)
        metrics_exporter.inflation_rate.set(inflation_rate)
        self.database_manager.update_api_table({
            'estimated_apy': inflation_rate / staked_fraction if staked_fraction else 0,
            'inflation_rate': inflation_rate,
            'total_staked_relay': total_staked,
        })

        return inflation_rate, staked_fraction

    def _get_era_first_block_number(self, era_id: int) -> int:
        """Find the first block of the active era."""
        logger.debug("[relay] Getting the first block of the era %s", era_id)
        era = None
        mid = None

        try:
            current_block_hash = self.service_params.substrate_relay__scalar_metrics.get_chain_head()
            current_block_number = self.service_params.substrate_relay__scalar_metrics.get_block_number(
                current_block_hash
            )
            start = 0
            if current_block_number - self.service_params.era_duration_in_blocks > 0:
                start = current_block_number - self.service_params.era_duration_in_blocks
            end = current_block_number
            while start <= end:
                mid = (start + end) // 2
                block_hash = self.service_params.substrate_relay__scalar_metrics.get_block_hash(mid)
                era = self.service_params.substrate_relay__scalar_metrics.query('Staking', 'ActiveEra', block_hash=block_hash)
                if era.value['index'] < era_id:
                    start = mid + 1
                else:
                    end = mid - 1
            if era.value['index'] == era_id:
                block_number = mid - 1
            else:
                block_number = mid
        except SubstrateRequestException as exc:
            logger.error("Can't find the required block")
            raise BlockNotFound from exc

        return block_number + 1

    def get_token_price(self) -> Union[float, None]:
        """Get the token price from the token price collector's database."""
        if self.database_manager_token_price_collector is None:
            return None

        logger.info("Getting the token price")
        date = datetime.now().strftime("%d-%m-%Y")
        token_price = self.database_manager_token_price_collector.get_token_price(date, self.service_params.token_symbol)
        if not token_price:
            logger.error("Failed to get token price: %s. Trying to get token price in the next block", date)
            metrics_exporter.alert_token_price_is_none.set(int(True))
            return None
        metrics_exporter.alert_token_price_is_none.set(int(False))
        logger.info("Token price per %s: %s", date, token_price[0])

        return token_price[0]
