"""This module contains the ValidatorsInfoExporter class, which """
import logging
import threading
import sys
import time

from substrateinterface.utils.ss58 import ss58_decode, ss58_encode

import utils

from contracts_reader import ContractsReader
from database_manager import DatabaseManager
from decorators import reconnection_counter
from prometheus_metrics import metrics_exporter
from service_parameters import ServiceParameters


SECOND = 1

logger = logging.getLogger(__name__)


class ValidatorsInfoExporter(threading.Thread):
    """A class that contains all the logic of reading and exporting validators info"""
    def __init__(self, database_manager: DatabaseManager, service_params: ServiceParameters):
        threading.Thread.__init__(self)
        self.database_manager = database_manager
        self.service_params = service_params
        self.contracts_reader = ContractsReader(service_params)
        self.lock = threading.Lock()
        self.stop = False

    def run(self):
        while True:
            self._run()

    @reconnection_counter.reconnection_counter
    def _run(self):
        """ValidatorsInfoExporter"""
        try:
            logger.info("Updating validators info list")
            validators_info = []
            stash_accounts = self.contracts_reader.get_stashes_and_ledgers()
            logger.info("Getting validators for each nominator")
            for stash, ledger in stash_accounts.items():
                validators = []
                nominator = self.service_params.substrate_relay__validators_info.query('Staking', 'Nominators', params=[stash])
                if nominator is not None and nominator.value is not None:
                    validators = nominator['targets'].value
                stash_decoded = ss58_decode(stash)
                stash_ss58 = ss58_encode(stash_decoded, ss58_format=self.service_params.ss58_format_para)

                validators_info.append({
                    "active_stake": self.contracts_reader.get_ledger_stake(ledger),
                    "ledger": ledger,
                    "stash": stash_ss58,
                    "validators": ','.join(validators),
                })
            self.database_manager.add_validators_info(validators_info)
            reconnection_counter.remove_thread(self._run.__doc__)
            self._wait(self.service_params.validators_info_exporter_waiting_time)
        except Exception as exc:
            metrics_exporter.alert_not_connected_validators_info_exporter.set(int(True))
            if type(exc) in utils.EXPECTED_NETWORK_EXCEPTIONS or isinstance(exc, NotImplementedError):
                logger.warning("An expected error occurred: %s - %s", type(exc), exc)
                utils.restore_connection_to_relay_chain(
                    timeout=self.service_params.timeout,
                    substrate=self.service_params.substrate_relay__validators_info,
                    ws_urls=self.service_params.ws_urls_para,
                )
            else:
                logger.critical("An unexpected error occurred: %s - %s", type(exc), exc)
            if not isinstance(exc, NotImplementedError):
                utils.restore_connection_to_parachain(
                    timeout=self.service_params.timeout,
                    ws_urls=self.service_params.ws_urls_para,
                    w3=self.service_params.w3__validators_info,
                )
            metrics_exporter.alert_not_connected_validators_info_exporter.set(int(False))

    def _wait(self, seconds: int):
        """Wait for N seconds"""
        # to prevent spam
        if seconds == SECOND:
            logger.debug("Waiting for %s seconds", seconds)
        else:
            logger.info("Waiting for %s seconds", seconds)

        for _ in range(seconds):
            with self.lock:
                if self.stop:
                    sys.exit()
            time.sleep(SECOND)
