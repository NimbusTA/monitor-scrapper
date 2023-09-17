"""This module contains the ContractsReader class which contains a set of methods of getting data from contracts."""
import logging

from dataclasses import dataclass
from typing import Any, NewType, Union
from eth_typing import Hash32, HexStr
from hexbytes import HexBytes
from substrateinterface.utils.ss58 import ss58_encode
from web3.contract import Contract
from web3.logs import DISCARD

from contract_event_filter import ContractEventFilter
from prometheus_metrics import metrics_exporter
from service_parameters import ServiceParameters


logger = logging.getLogger(__name__)
BlockIdentifier = NewType('BlockIdentifier', Union[int, str])


@dataclass
class ContractsReader:
    """This class contains methods of getting data from contracts"""
    service_params: ServiceParameters

    nimbus_event_filter: ContractEventFilter = None

    nimbus__aggregated_metrics: Contract = None
    nimbus__scalar_metrics: Contract = None
    nimbus__trading_volume: Contract = None
    nimbus__validators_info: Contract = None

    oracle_master__scalar_metrics: Contract = None
    withdrawal__scalar_metrics: Contract = None
    xctoken__scalar_metrics: Contract = None

    deposited_event_hash: str = None
    losses_events_hash: str = None
    redeemed_events_hash: str = None
    rewards_event_hash: str = None
    swap_event_hash: str = None
    token_exchange_event_hash: str = None
    transfer_event_hash: str = None

    def __post_init__(self):
        self.initialize_contracts(True, True, True)

        # self.add_liquidity_event_hash = self.service_params.w3__aggregated_metrics_exporter.keccak(
        #     text="AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)",
        # )
        self.deposited_event_hash = self.service_params.w3__scalar_metrics.keccak(text="Deposited(address,uint256)")
        self.losses_event_hash = self.service_params.w3__scalar_metrics.keccak(text="Losses(address,uint256,uint256)")
        self.redeemed_event_hash = self.service_params.w3__scalar_metrics.keccak(text="Redeemed(address,uint256)")
        # self.remove_liquidity_one_event_hash = self.service_params.w3__scalar_metric_exporter.keccak(
        #     text="RemoveLiquidityOne(address,uint256,uint256,uint256)")
        self.rewards_event_hash = self.service_params.w3__scalar_metrics.keccak(text="Rewards(address,uint256,uint256)")
        # self.swap_event_hash = self.service_params.w3__scalar_metric_exporter.keccak(
        #     text="Swap(address,uint256,uint256,uint256,uint256,address)")
        # self.token_exchange_event_hash = self.service_params.w3__scalar_metric_exporter.keccak(
        #     text="TokenExchange(address,int128,uint256,int128,uint256)")
        self.transfer_event_hash = self.service_params.w3__scalar_metrics.keccak(text="Transfer(address,address,uint256)")

    def initialize_contracts(self,
                             for_aggregated_metrics: bool = False,
                             for_scalar_metrics: bool = False,
                             for_validators_info: bool = False):
        """Initialize or reinitialize contract instances."""
        if for_aggregated_metrics:
            logger.info("Initializing contracts for the AggregatedMetricsExporter")
            self.nimbus__aggregated_metrics = self.service_params.w3__aggregated_metrics.eth.contract(
                address=self.service_params.nimbus_address,
                abi=self.service_params.nimbus_abi,
            )

        if for_scalar_metrics:
            logger.info("Initializing contracts for the ScalarMetricsExporter")
            self.nimbus__scalar_metrics = self.service_params.w3__scalar_metrics.eth.contract(
                address=self.service_params.nimbus_address,
                abi=self.service_params.nimbus_abi,
            )
            self.oracle_master__scalar_metrics = self.service_params.w3__scalar_metrics.eth.contract(
                address=self.service_params.oracle_master_address,
                abi=self.service_params.oracle_master_abi,
            )
            self.withdrawal__scalar_metrics = self.service_params.w3__scalar_metrics.eth.contract(
                address=self.service_params.withdrawal_address,
                abi=self.service_params.withdrawal_abi,
            )
            self.xctoken__scalar_metrics = self.service_params.w3__scalar_metrics.eth.contract(
                address=self.service_params.xctoken_address,
                abi=self.service_params.xctoken_abi,
            )

        if for_validators_info:
            logger.info("Initializing contracts for the ValidatorsInfoExporter")
            self.nimbus__validators_info = self.service_params.w3__validators_info.eth.contract(
                address=self.service_params.nimbus_address,
                abi=self.service_params.nimbus_abi,
            )

    @staticmethod
    def _call_method(block_identifier: int or str, method: Any) -> Any:
        """Call the provided method and return the result."""
        try:
            result = method.call(block_identifier=block_identifier)
        except Exception as exc:
            logger.critical("[block %s] Failed to call the method %s: %s", block_identifier, method, exc)
            raise exc from exc

        return result

    def get_oracle_members(self, block_number: int) -> list:
        """Get the list of oracle members."""
        logger.info("Getting Oracle members")
        members = []
        max_oracle_members = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.MAX_MEMBERS())
        logger.info("Maximum possible number of oracle members: %s", max_oracle_members)

        for member_number in range(max_oracle_members):
            method = self.oracle_master__scalar_metrics.functions.members(member_number)
            try:
                member = method.call(block_identifier=block_number)
            except ValueError:
                break
            except Exception as exc:
                logger.critical("[block %s] Failed to call the method %s: %s", block_number, method, exc)
                raise exc
            members.append(member)
            logger.debug("Added %s to the oracle members list", member)
        logger.info("Oracle members: %s", members)

        return members

    def get_and_expose_oracle_balances(self, block_number: int):
        """Get the list of oracle members, then read and expose their balances to Prometheus."""
        logger.info("Getting Oracle balances")
        balances = {}
        oracle_members = self.get_oracle_members(block_number)
        for member in oracle_members:
            try:
                balance = self.service_params.w3__scalar_metrics.eth.get_balance(member, block_identifier=block_number)
            except Exception as exc:
                logger.critical("[block %s] Failed to call the method w3.eth.get_balance: %s", block_number, exc)
                raise exc from exc
            balances[member] = balance
            metrics_exporter.oracle_service_balance.labels(member).set(balance)
        logger.info("Oracle balances: %s", balances)

    def get_and_expose_next_era_start_time_in_contract(self, block_number: int):
        """Get the next era start time in the OracleMaster contract and expose it to Prometheus."""
        logger.info("Getting the next era start time in contract")

        anchor_era_id = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.ANCHOR_ERA_ID())
        anchor_timestamp = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.ANCHOR_TIMESTAMP())
        era_id = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.getCurrentEraId())
        seconds_per_era = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.SECONDS_PER_ERA())

        next_era_start_time_in_contract = (era_id + 1 - anchor_era_id) * seconds_per_era + 10 * 60 + anchor_timestamp
        metrics_exporter.oracle_master_next_era_start_time.set(next_era_start_time_in_contract)
        logger.info("The next era start time in the OracleMaster: %s", next_era_start_time_in_contract)

    def get_and_expose_era_values_from_oracle_master(self, block_number: int):
        """Get the eraId and getCurrentEraId values from the OracleMaster contract and expose them to Prometheus."""
        logger.info("Getting current era id from OracleMaster")

        current_era_id = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.getCurrentEraId())
        metrics_exporter.oracle_master_current_era_id.set(current_era_id)
        logger.info("The currentEraId in the OracleMaster: %s", current_era_id)

        era_id = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.eraId())
        metrics_exporter.oracle_master_era_id.set(era_id)
        logger.info("The eraId in the OracleMaster: %s", era_id)

    def get_and_expose_nimbus_metrics(self, block_number: int, token_price: float = None) -> int:
        """Get the Nimbus metrics and expose them to Prometheus. Returns total supply"""
        logger.info("Getting Nimbus metrics")

        total_supply = self._call_method(block_number, self.nimbus__scalar_metrics.functions.totalSupply())
        metrics_exporter.nimbus_total_supply.set(total_supply)

        if token_price:
            total_supply_usd = total_supply * token_price
            logger.info("Nimbus total supply in USD: %s", total_supply_usd)
            metrics_exporter.nimbus_total_supply_usd.set(total_supply_usd)
        else:
            logger.warning("Can't calculate the total supply in USD: the token price is not provided")

        token_balance = self._call_method(block_number,
                                          self.nimbus__scalar_metrics.functions.balanceOf(self.service_params.nimbus_address))
        metrics_exporter.nimbus_tokens.set(token_balance)

        buffered_deposits = self._call_method(block_number, self.nimbus__scalar_metrics.functions.bufferedDeposits())
        metrics_exporter.nimbus_buffered_deposits.set(buffered_deposits)

        buffered_redeems = self._call_method(block_number, self.nimbus__scalar_metrics.functions.bufferedRedeems())
        metrics_exporter.nimbus_buffered_redeems.set(buffered_redeems)

        return total_supply

    def get_and_expose_withdrawal_metrics(self, block_number: int):
        """Get metrics from the Withdrawal contract and expose them to Prometheus."""
        logger.info("Getting Withdrawal metrics")

        tokens = self._call_method(block_number,
                                   self.xctoken__scalar_metrics.functions.balanceOf(self.service_params.withdrawal_address))
        metrics_exporter.withdrawal_tokens.set(tokens)

        pending_for_claiming = self._call_method(block_number, self.withdrawal__scalar_metrics.functions.pendingForClaiming())
        metrics_exporter.withdrawal_pending_for_claiming.set(pending_for_claiming)

        total_virtual_xctoken_amount = self._call_method(block_number,
                                                         self.withdrawal__scalar_metrics.functions.totalVirtualXcTokenAmount())
        metrics_exporter.withdrawal_total_virtual_xctoken_amount.set(total_virtual_xctoken_amount)

        total_xctoken_pool_shares = self._call_method(block_number,
                                                      self.withdrawal__scalar_metrics.functions.totalXcTokenPoolShares())
        metrics_exporter.withdrawal_total_xctoken_pool_shares.set(total_xctoken_pool_shares)

    def get_nimbus_events(self, from_block: int, to_block: int) -> dict:
        """Get events from the Nimbus contract."""
        logger.info("Getting Nimbus events")
        if self.nimbus_event_filter is not None:
            self.nimbus_event_filter.update_filter_range(from_block, to_block)
        else:
            self.nimbus_event_filter = ContractEventFilter(
                self.service_params.w3__aggregated_metrics,
                self.service_params.nimbus_address,
                from_block,
                to_block,
                events_to_find=set('nimbus',),
            )
        events_mixed = self.nimbus_event_filter.get_all_entries()

        return self.distribute_events(events_mixed)

    @staticmethod
    def find_log(logs: tuple, log_index: int) -> int:
        """Find the log by its index."""
        for index, log in enumerate(logs):
            if log_index == log.get('logIndex'):
                return index

        return 0

    def _get_transaction_receipt(self, transaction_hash: Union[Hash32, HexBytes, HexStr]) -> Any:
        """Get the transaction receipt."""
        try:
            tx_receipt = self.service_params.w3__aggregated_metrics.eth.get_transaction_receipt(transaction_hash)
        except Exception as exc:
            logger.critical("Failed to call the method get_transaction_receipt: %s", exc)
            raise exc from exc

        return tx_receipt

    @staticmethod
    def _process_receipt(method: Any, tx_receipt: str) -> Any:
        """Process the transaction receipt and get rich logs."""
        try:
            rich_logs = method.processReceipt(tx_receipt, errors=DISCARD)
        except Exception as exc:
            logger.critical("Failed to call the method processReceipt: %s", exc)
            raise exc from exc

        return rich_logs

    # def distribute_events(self, events_mixed: list, destination: str = None) -> dict:
    def distribute_events(self, events_mixed: list) -> dict:
        """Distribute events by type and order by log index."""
        if not events_mixed:
            return {}

        events = {
            # 'AddLiquidity': [],
            'Deposited': [],
            'Losses': [],
            'Redeemed': [],
            # 'RemoveLiquidityOne': [],
            'Rewards': [],
            # 'Swap': [],
            # 'TokenExchange': [],
            'Transfer': [],
        }

        for event in events_mixed:
            topics = event.get('topics')
            log_index = event.get('logIndex')
            tx_receipt = self._get_transaction_receipt(event.get('transactionHash'))
            for topic in topics:
                if topic == self.deposited_event_hash:
                    rich_logs = self._process_receipt(self.nimbus__aggregated_metrics.events.Deposited(), tx_receipt)
                    events['Deposited'].append(rich_logs[self.find_log(rich_logs, log_index)])
                    break
                if topic == self.losses_event_hash:
                    rich_logs = self._process_receipt(self.nimbus__aggregated_metrics.events.Losses(), tx_receipt)
                    events['Losses'].append(rich_logs[self.find_log(rich_logs, log_index)])
                    break
                if topic == self.redeemed_event_hash:
                    rich_logs = self._process_receipt(self.nimbus__aggregated_metrics.events.Redeemed(), tx_receipt)
                    events['Redeemed'].append(rich_logs[self.find_log(rich_logs, log_index)])
                    break
                if topic == self.rewards_event_hash:
                    rich_logs = self._process_receipt(self.nimbus__aggregated_metrics.events.Rewards(), tx_receipt)
                    events['Rewards'].append(rich_logs[self.find_log(rich_logs, log_index)])
                    break
                if topic == self.transfer_event_hash:
                    events['Transfer'].append(event)
                    break

        return events

    def get_and_expose_ledger_metrics(self, block_number: int, ledger_addresses: set = None):
        """Get ledger metrics and expose them to Prometheus."""
        logger.info("Getting ledgers metrics")
        if ledger_addresses is None:
            logger.info("Ledger addresses are not provided")
            return

        ledgers_stake = 0
        for ledger in ledger_addresses:
            ledger_contract = self.service_params.w3__scalar_metrics.eth.contract(ledger, abi=self.service_params.ledger_abi)

            total_balance = self._call_method(block_number, ledger_contract.functions.totalBalance())
            metrics_exporter.parachain_ledger_total_balance.labels(ledger).set(total_balance)

            locked_balance = self._call_method(block_number, ledger_contract.functions.lockedBalance())
            metrics_exporter.parachain_ledger_locked_balance.labels(ledger).set(locked_balance)

            active_balance = self._call_method(block_number, ledger_contract.functions.activeBalance())
            metrics_exporter.parachain_ledger_active_balance.labels(ledger).set(active_balance)

            status = self._call_method(block_number, ledger_contract.functions.status())
            metrics_exporter.parachain_ledger_status.labels(ledger).set(status)

            transfer_upward_balance = self._call_method(block_number, ledger_contract.functions.transferUpwardBalance())
            metrics_exporter.parachain_ledger_transfer_upward_balance.labels(ledger).set(transfer_upward_balance)

            transfer_downward_balance = self._call_method(block_number, ledger_contract.functions.transferDownwardBalance())
            metrics_exporter.parachain_ledger_transfer_downward_balance.labels(ledger).set(transfer_downward_balance)

            cached_total_balance = self._call_method(block_number, ledger_contract.functions.cachedTotalBalance())
            metrics_exporter.parachain_ledger_cached_total_balance.labels(ledger).set(cached_total_balance)

            ledger_borrow = self._call_method(block_number, self.nimbus__scalar_metrics.functions.ledgerBorrow(ledger))
            metrics_exporter.parachain_ledger_borrow.labels(ledger).set(ledger_borrow)

            ledger_stake = self._call_method(block_number, self.nimbus__scalar_metrics.functions.ledgerStake(ledger))
            metrics_exporter.parachain_ledger_stake.labels(ledger).set(ledger_stake)
            ledgers_stake += ledger_stake

            ledger_xctoken_balance = self._call_method(block_number, self.xctoken__scalar_metrics.functions.balanceOf(ledger))
            metrics_exporter.parachain_ledger_xctoken_balance.labels(ledger).set(ledger_xctoken_balance)
        metrics_exporter.parachain_ledgers_stake.set(ledgers_stake)

    def get_ledger_addresses(self, block_identifier: BlockIdentifier = 'latest') -> set:
        """Get ledger addresses from the specified block, including removed ones."""
        logger.info("Getting Ledger addresses")
        if block_identifier == 'latest':
            nimbus_contract = self.nimbus__aggregated_metrics
        else:
            nimbus_contract = self.nimbus__scalar_metrics

        ledger_addresses = self._call_method(block_identifier, nimbus_contract.functions.getLedgerAddresses())
        logger.info("Ledger addresses: %s", ledger_addresses)

        return set(ledger_addresses)

    def get_and_expose_controller_balance(self, block_number: int):
        """Get and expose to Prometheus the Controller balance."""
        logger.info("Getting the controller balance")
        controller_balance = self._call_method(block_number,
                                               self.xctoken__scalar_metrics.functions.balanceOf(
                                                   self.service_params.controller_address))
        metrics_exporter.controller_balance.set(controller_balance)
        logger.info("The controller balance: %s", controller_balance)

    def get_stashes_and_ledgers(self, block_number: int = None) -> dict:
        """Get dicts with the stash key and the ledger value."""
        logger.info("Getting ledger addresses in relay chain and parachain")
        stashes = {}

        if block_number is None:
            stash_accounts = self._call_method('latest', self.nimbus__validators_info.functions.getStashAccounts())
            for stash in stash_accounts:
                stashes[ss58_encode(stash)] = self._call_method('latest',
                                                                self.nimbus__validators_info.functions.findLedger(stash))
        else:
            stash_accounts = self._call_method(block_number, self.oracle_master__scalar_metrics.functions.getStashAccounts())
            for stash in stash_accounts:
                stashes[stash] = self._call_method(block_number, self.nimbus__scalar_metrics.functions.findLedger(stash))
        logger.debug("Ledgers and its stashes: %s", stashes)

        return stashes

    def get_ledger_stake(self, ledger: str) -> int:
        """Get the ledger stake from the Nimbus contract."""
        logger.info("Getting the stake of the ledger %s", ledger)
        ledger_stake = self._call_method('latest', self.nimbus__validators_info.functions.ledgerStake(ledger))

        return ledger_stake

    def get_pooled_tokens_by_shares(self, shares: int, block_number: int, destination: str = 'nimbus') -> int:
        """Get pooled Token by shares from the Nimbus contract."""
        logger.info("Getting pooled tokens by shares")
        if destination == 'trading_volume_exporter':
            method = self.nimbus__trading_volume.functions.getPooledTokenByShares(shares)
        else:
            method = self.nimbus__scalar_metrics.functions.getPooledTokenByShares(shares)

        pooled_tokens_by_shares = self._call_method(block_number, method)
        logger.info("Pooled tokens by shares: %s", pooled_tokens_by_shares)

        return pooled_tokens_by_shares
