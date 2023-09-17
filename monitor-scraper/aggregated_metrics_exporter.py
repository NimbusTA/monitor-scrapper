"""This module contains the AggregatedMetricsExporter class, which collects and process events from the Nimbus contract"""
import asyncio
import logging
import sys

from threading import Lock, Thread
from time import sleep
from prometheus_metrics import metrics_exporter

import utils

from contracts_reader import ContractsReader
from database_manager import DatabaseManager
from decorators import reconnection_counter
from service_parameters import ServiceParameters

SECOND = 1

logger = logging.getLogger(__name__)


class AggregatedMetricsExporter(Thread):
    """This class contains a set of methods for scraping and processing events from the Nimbus contract."""
    database_manager: DatabaseManager
    service_params: ServiceParameters

    apr: float
    contracts_reader: ContractsReader
    deposited: int
    deposited_events_num: int
    holders_number: int
    initialized: bool
    last_block_number_with_events: int
    lock: Lock
    losses: int
    redeemed: int
    redeemed_events_num: int
    rewards: int
    stop: bool

    def __init__(self, database_manager: DatabaseManager, service_params: ServiceParameters):
        Thread.__init__(self)
        self.database_manager = database_manager
        self.service_params = service_params

        self.apr = 0.
        self.contracts_reader = ContractsReader(self.service_params)
        self.deposited = 0
        self.deposited_events_num = 0
        self.holders_number = 0
        self.initialized = False
        self.last_block_number_with_events = -1
        self.lock = Lock()
        self.losses = 0
        self.redeemed = 0
        self.redeemed_events_num = 0
        self.rewards = 0
        self.stop = False

    def run(self):
        try:
            from_block = self.restore_from_database()
        except Exception as exc:
            logger.warning("Error during recovery from the database: %s", exc)
            from_block = self.service_params.initial_block_num_aggregated_metrics
        self.initialized = True

        logger.info("Starting from block: %s", from_block)
        current_range_of_blocks = self.service_params.max_range_of_blocks
        while True:
            from_block, current_range_of_blocks = self._run(from_block, current_range_of_blocks)

    @reconnection_counter.reconnection_counter
    def _run(self, from_block: int, current_range_of_blocks: int) -> (int, int):
        """AggregatedMetricsExporter"""
        try:
            with self.lock:
                if self.stop:
                    sys.exit()

            block_hash = self.service_params.substrate_para__aggregated_metrics.get_chain_finalised_head()
            block_number = self.service_params.substrate_para__aggregated_metrics.get_block_number(block_hash)
            if block_number <= from_block:
                sleep(SECOND)
                reconnection_counter.remove_thread(self._run.__doc__)
                return from_block, current_range_of_blocks

            to_block = from_block + current_range_of_blocks
            if block_number < to_block:
                to_block = block_number

            from_block = self.get_and_process_aggregated_data(from_block, to_block)
            if current_range_of_blocks != self.service_params.max_range_of_blocks:
                logger.info("Reset current range of blocks up to %s", self.service_params.max_range_of_blocks)
                current_range_of_blocks = self.service_params.max_range_of_blocks
            reconnection_counter.remove_thread(self._run.__doc__)
        except Exception as exc:
            if type(exc) in utils.EXPECTED_NETWORK_EXCEPTIONS:
                logger.warning("An expected error occurred: %s - %s", type(exc), exc)
            elif isinstance(exc, asyncio.exceptions.TimeoutError) or isinstance(exc, ValueError) and exc.args and \
                    'message' in exc.args[0] and exc.args[0]['message'] == 'query returned more than 10000 results':
                logger.warning("The block range is too big: decreasing")
                current_range_of_blocks = utils.decrease_range_of_blocks(current_range_of_blocks)
                reconnection_counter.remove_thread(self._run.__doc__)
                return from_block, current_range_of_blocks
            else:
                logger.critical("An unexpected error occurred: %s - %s", type(exc), exc)
            metrics_exporter.alert_not_connected_aggregated_metrics_exporter.set(int(True))
            self.service_params.substrate_para__aggregated_metrics, \
                self.service_params.w3__aggregated_metrics = \
                utils.restore_connection_to_parachain(
                    substrate=self.service_params.substrate_para__aggregated_metrics,
                    timeout=self.service_params.timeout,
                    w3=self.service_params.w3__aggregated_metrics,
                    ws_urls=self.service_params.ws_urls_para,
                )
            self.contracts_reader.initialize_contracts(for_aggregated_metrics=True)
            metrics_exporter.alert_not_connected_aggregated_metrics_exporter.set(int(False))

        return from_block, current_range_of_blocks

    def get_and_process_aggregated_data(self, from_block: int, to_block: int) -> int:
        """Get events from the Nimbus contract in the specific range, parse and process them.
        Returns the next block that is going to be processed.
        """
        logger.info("Processing block range: [%s; %s]", from_block, to_block)
        events = self.contracts_reader.get_nimbus_events(from_block, to_block)
        distributed_events = utils.distribute_events_by_blocks(events)
        if not distributed_events:
            logger.info("Events not found")
        for block_number, events in distributed_events.items():
            self._process_deposited_and_redeemed_events(events)
            rewards = self._process_rewards_and_losses_events(events['Rewards'] + events['Losses'])
            self._process_transfer_events(events)

            entry = {
                'deposited': self.deposited,
                'deposited_events_num': self.deposited_events_num,
                'redeemed': self.redeemed,
                'redeemed_events_num': self.redeemed_events_num,
                'last_processed_block_number_with_events': block_number,
            }
            self.database_manager.update_aggregated_data(entry)

            for reward in rewards:
                self.database_manager.add_reward(reward['ledger'], reward['reward'], reward['balance'], block_number)

            rewards = {}
            ledger_addresses = self.contracts_reader.get_ledger_addresses()
            for ledger in ledger_addresses:
                ledger_rewards = self.database_manager.get_rewards(ledger, self.service_params.database_query_limit)
                if ledger_rewards:
                    rewards[ledger] = ledger_rewards
            self.apr = utils.calculate_apr(
                self.apr, rewards, self.service_params.eras_per_day, self.service_params.apr_min, self.service_params.apr_max)
            metrics_exporter.apr.set(self.apr)

            self.last_block_number_with_events = block_number
            metrics_exporter.parachain_last_block_number_with_events.set(self.last_block_number_with_events)
        self.database_manager.update_api_table({'apr': self.apr, 'total_rewards': self.rewards, 'total_losses': self.losses})

        return to_block + 1

    def _process_deposited_and_redeemed_events(self, events: dict):
        """Process Deposited and Redeemed events."""
        logger.info("Deposited events: %s", events['Deposited'])
        logger.info("Redeemed events: %s", events['Redeemed'])
        self.deposited_events_num += len(events['Deposited'])
        metrics_exporter.nimbus_deposited_events_number.set(self.deposited_events_num)
        self.redeemed_events_num += len(events['Redeemed'])
        metrics_exporter.nimbus_redeemed_events_number.set(self.redeemed_events_num)

        for event in events['Deposited']:
            self.deposited += event.get('args').get('amount')
        metrics_exporter.nimbus_deposited.set(self.deposited)

        for event in events['Redeemed']:
            self.redeemed += event.get('args').get('amount')
        metrics_exporter.nimbus_redeemed.set(self.redeemed)

    def _process_transfer_events(self, events: dict):
        """Process Transfer events."""
        logger.info("Transfer events: %s", events['Transfer'])
        for transfer_event in events['Transfer']:
            receiver = '0x' + str(transfer_event.get('topics')[2].hex())[26:]
            self.database_manager.add_holder(receiver)
        self.holders_number = self.database_manager.get_holders_number()[0]
        metrics_exporter.holders_number.set(self.holders_number)

    def _process_rewards_and_losses_events(self, events: dict) -> list:
        """Process Rewards and Losses events."""
        logger.info("Rewards and Losses events: %s", events)
        rewards = []

        for event in events:
            event_type = event.get('event')
            ledger = event.get('args').get('ledger')
            balance = event.get('args').get('balance')

            if event_type == 'Rewards':
                reward = event.get('args').get('rewards')
                self.rewards += event.get('args').get('rewards')
                metrics_exporter.nimbus_rewards_aggregated.set(self.rewards)
            else:
                reward = -event.get('args').get('losses')
                self.losses += event.get('args').get('losses')
                metrics_exporter.nimbus_losses_aggregated.set(self.losses)

            rewards.append({'ledger': ledger, 'reward': reward, 'balance': balance})

        return rewards

    def restore_from_database(self) -> int:
        """Restore data from the database.
        Returns the next block that is going to be processed.
        """
        logger.info("Recovering data from the database")
        self.holders_number = self.database_manager.get_holders_number()[0]
        metrics_exporter.holders_number.set(self.holders_number)

        rewards = {}
        ledger_addresses = self.database_manager.get_ledger_addresses()
        for ledger in ledger_addresses:
            ledger_rewards = self.database_manager.get_rewards(ledger[0], self.service_params.database_query_limit)
            if ledger_rewards:
                rewards[ledger[0]] = ledger_rewards
        self.apr = utils.calculate_apr(
            self.apr, rewards, self.service_params.eras_per_day, self.service_params.apr_min, self.service_params.apr_max)

        aggregated_data = self.database_manager.get_aggregated_data()
        if not aggregated_data:
            self.deposited = 0
            self.deposited_events_num = 0
            self.redeemed = 0
            self.redeemed_events_num = 0
            from_block = self.service_params.initial_block_num_aggregated_metrics
        else:
            self.deposited = int(aggregated_data[0])
            self.deposited_events_num = aggregated_data[1]
            self.redeemed = int(aggregated_data[2])
            self.redeemed_events_num = aggregated_data[3]
            self.last_block_number_with_events = aggregated_data[4]
            metrics_exporter.parachain_last_block_number_with_events.set(self.last_block_number_with_events)
            from_block = self.last_block_number_with_events + 1

        metrics_exporter.apr.set(self.apr)
        metrics_exporter.nimbus_deposited.set(self.deposited)
        metrics_exporter.nimbus_deposited_events_number.set(self.deposited_events_num)
        metrics_exporter.nimbus_redeemed.set(self.redeemed)
        metrics_exporter.nimbus_redeemed_events_number.set(self.redeemed_events_num)

        rewards, losses = self.database_manager.get_all_rewards_and_losses()
        if rewards:
            for reward in rewards:
                self.rewards += reward[0]
        else:
            self.rewards = 0
            metrics_exporter.nimbus_rewards_aggregated.set(self.rewards)
        if losses:
            for loss in losses:
                self.losses += loss[0]
        else:
            self.losses = 0
            metrics_exporter.nimbus_losses_aggregated.set(self.losses)

        logger.info("Recovery from the database completed successfully")

        return from_block
