"""This module contains the ServiceParameters class implementation and constants."""
import json
import logging
import os
import sys
import urllib

from typing import List

from eth_typing import ChecksumAddress
from substrateinterface import SubstrateInterface
from web3.main import Web3

import log
import utils

from abi_checker import ABIChecker
from database_manager import DatabaseManager
from database_manager_token_price_collector import DatabaseManagerTokenPriceCollector


DEFAULT_LEDGER_CONTRACT_ABI_PATH = './assets/Ledger.json'
DEFAULT_NIMBUS_CONTRACT_ABI_PATH = './assets/Nimbus.json'
DEFAULT_ORACLE_MASTER_CONTRACT_ABI_PATH = './assets/OracleMaster.json'
DEFAULT_WITHDRAWAL_CONTRACT_ABI_PATH = './assets/Withdrawal.json'
DEFAULT_XCTOKEN_CONTRACT_ABI_PATH = './assets/xcTOKEN.json'

DEFAULT_APR_MIN = '3'
DEFAULT_APR_MAX = '45'
DEFAULT_DATABASE_QUERY_LIMIT = '100'
DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_MAX_RANGE_OF_BLOCKS = '2500'
DEFAULT_PROMETHEUS_METRICS_PORT = '8000'
DEFAULT_SS58_FORMAT = '42'
DEFAULT_TIMEOUT = '60'
DEFAULT_VALIDATORS_INFO_EXPORTER_WAITING_TIME = '60'

HUNDRED_PERCENT = 100
MAX_ATTEMPTS_TO_RECONNECT = 20

logger = logging.getLogger(__name__)


class ServiceParameters:
    """This class contains the service parameters and methods to check and parse them."""
    controller_address: ChecksumAddress
    nimbus_address: ChecksumAddress
    oracle_master_address: ChecksumAddress
    payout_service_address: str
    withdrawal_address: ChecksumAddress
    xctoken_address: ChecksumAddress

    auction_adjust: float
    auction_max: int
    falloff: float
    max_inflation: float
    min_inflation: float
    stake_target: float

    apr: float or None
    apr_max: float
    apr_min: float
    token_symbol: str

    max_range_of_blocks: int
    initial_block_num_aggregated_metrics: int

    era_duration_in_blocks: int
    eras_per_day: int

    database_query_limit: int
    database_url: str
    database_url_token_price_collector: str
    prometheus_metrics_port: int
    validators_info_exporter_waiting_time: int

    ss58_format_para: int
    ss58_format_relay: int
    timeout: int
    type_registry_preset_para: str
    type_registry_preset_relay: str
    ws_urls_para: list
    ws_urls_relay: list

    substrate_para__aggregated_metrics: SubstrateInterface
    substrate_para__scalar_metrics: SubstrateInterface

    substrate_relay__scalar_metrics: SubstrateInterface
    substrate_relay__validators_info: SubstrateInterface

    w3__aggregated_metrics: Web3
    w3__validators_info: Web3

    def __init__(self):
        log_level = os.getenv('LOG_LEVEL', DEFAULT_LOG_LEVEL)
        self._check_log_level(log_level)
        log.init_log(log_level)

        logger.info("Checking configuration parameters")

        logger.info("[ENV] LOG_LEVEL: %s", log_level)

        logger.info("Checking URLs")
        logger.info("[ENV] Get 'WS_URLS_PARA'")
        self.ws_urls_para = os.getenv('WS_URLS_PARA').split(',')
        assert self.is_valid_urls(self.ws_urls_para), "Invalid urls were found in the 'WS_URLS_PARA' parameter"
        logger.info("[ENV] WS_URLS_PARA: successfully got %s url(s)", len(self.ws_urls_para))

        logger.info("[ENV] Get 'WS_URLS_RELAY'")
        self.ws_urls_relay = os.getenv('WS_URLS_RELAY').split(',')
        assert self.is_valid_urls(self.ws_urls_relay), "Invalid urls were found in the 'WS_URLS_RELAY' parameter"
        logger.info("[ENV] WS_URLS_RELAY: successfully got %s url(s)", len(self.ws_urls_relay))
        logger.info("URLs checked")

        logger.info("Checking paths to ABIs")
        ledger_abi_path = os.getenv('LEDGER_CONTRACT_ABI_PATH', DEFAULT_LEDGER_CONTRACT_ABI_PATH)
        nimbus_abi_path = os.getenv('NIMBUS_CONTRACT_ABI_PATH', DEFAULT_NIMBUS_CONTRACT_ABI_PATH)
        oracle_master_abi_path = os.getenv('ORACLE_MASTER_CONTRACT_ABI_PATH', DEFAULT_ORACLE_MASTER_CONTRACT_ABI_PATH)
        withdrawal_abi_path = os.getenv('WITHDRAWAL_CONTRACT_ABI_PATH', DEFAULT_WITHDRAWAL_CONTRACT_ABI_PATH)
        xctoken_abi_path = os.getenv('XCTOKEN_CONTRACT_ABI_PATH', DEFAULT_XCTOKEN_CONTRACT_ABI_PATH)

        ABIChecker.check_abi_path(
            ledger_abi_path,
            nimbus_abi_path,
            oracle_master_abi_path,
            withdrawal_abi_path,
            xctoken_abi_path,
        )
        logger.info("Paths to ABIs checked")

        logger.info("Checking parameters for the APY calculation")
        logger.info("[ENV] Get 'AUCTION_ADJUST'")
        auction_adjust = os.getenv('AUCTION_ADJUST')
        assert auction_adjust, "The 'AUCTION_ADJUST' parameter is not specified"
        self.auction_adjust = float(auction_adjust)
        logger.info("[ENV] 'AUCTION_ADJUST': %s", self.auction_adjust)

        logger.info("[ENV] Get 'AUCTION_MAX'")
        auction_max = os.getenv('AUCTION_MAX')
        assert auction_max, "The 'AUCTION_MAX' parameter is not specified"
        self.auction_max = int(auction_max)
        logger.info("[ENV] 'AUCTION_MAX': %s", self.auction_max)

        logger.info("[ENV] Get 'FALLOFF'")
        falloff = os.getenv('FALLOFF')
        assert falloff, "The 'FALLOFF' parameter is not specified"
        self.falloff = float(falloff)
        logger.info("[ENV] 'FALLOFF': %s", self.falloff)

        logger.info("[ENV] Get 'MAX_INFLATION'")
        max_inflation = os.getenv('MAX_INFLATION')
        assert max_inflation, "The 'MAX_INFLATION' parameter is not specified"
        self.max_inflation = float(max_inflation)
        logger.info("[ENV] 'MAX_INFLATION': %s", self.max_inflation)

        logger.info("[ENV] Get 'MIN_INFLATION'")
        min_inflation = os.getenv('MIN_INFLATION')
        assert min_inflation, "The 'MIN_INFLATION' parameter is not specified"
        self.min_inflation = float(min_inflation)
        logger.info("[ENV] 'MIN_INFLATION': %s", self.min_inflation)

        logger.info("[ENV] Get 'STAKE_TARGET'")
        stake_target = os.getenv('STAKE_TARGET')
        assert stake_target, "The 'STAKE_TARGET' parameter is not specified"
        self.stake_target = float(stake_target)
        logger.info("[ENV] 'STAKE_TARGET': %s", self.stake_target)
        logger.info("Parameters for the APY calculation checked")

        logger.info("Checking other parameters")
        logger.info("[ENV] Get 'APR'")
        self.apr = float(os.getenv('APR')) if os.getenv('APR') else None
        logger.info("[ENV] APR: %s", self.apr)

        apr_min = int(os.getenv('APR_MIN', DEFAULT_APR_MIN))
        assert apr_min >= 0, "The 'apr_min' parameter must be a non-negative integer"
        apr_max = int(os.getenv('APR_MAX', DEFAULT_APR_MAX))
        assert apr_max >= 0, "The 'apr_min' parameter must be a non-negative integer"
        assert apr_max >= apr_min, "The 'apr_min' parameter is greater than the 'apr_max'"
        self.apr_min = apr_min / HUNDRED_PERCENT
        self.apr_max = apr_max / HUNDRED_PERCENT

        self.token_symbol = os.getenv('TOKEN_PRICE_COLLECTOR_TOKEN_SYMBOL')
        if self.token_symbol:
            assert self.token_symbol in ('dot', 'glmr', 'ksm', 'movr'), \
                f"Provided an invalid token symbol: {self.token_symbol}"
            logger.info("[ENV] TOKEN_PRICE_COLLECTOR_TOKEN_SYMBOL: %s", self.token_symbol)
        else:
            logger.info("[ENV] TOKEN_PRICE_COLLECTOR_TOKEN_SYMBOL: not provided")

        logger.info("[ENV] Get 'SS58_FORMAT_PARA'")
        self.ss58_format_para = int(os.getenv('SS58_FORMAT_PARA', DEFAULT_SS58_FORMAT))
        logger.info("[ENV] 'SS58_FORMAT_PARA': %s", self.ss58_format_para)

        logger.info("[ENV] Get 'SS58_FORMAT_RELAY'")
        self.ss58_format_relay = int(os.getenv('SS58_FORMAT_RELAY', DEFAULT_SS58_FORMAT))
        logger.info("[ENV] 'SS58_FORMAT_RELAY': %s", self.ss58_format_relay)

        logger.info("[ENV] Get 'TYPE_REGISTRY_PRESET_PARA'")
        self.type_registry_preset_para = os.getenv('TYPE_REGISTRY_PRESET_PARA')
        assert self.type_registry_preset_para, "The 'TYPE_REGISTRY_PRESET_PARA' parameter is not provided"
        logger.info("[ENV] 'TYPE_REGISTRY_PRESET_PARA': %s", self.type_registry_preset_para)

        logger.info("[ENV] Get 'TYPE_REGISTRY_PRESET_RELAY'")
        self.type_registry_preset_relay = os.getenv('TYPE_REGISTRY_PRESET_RELAY')
        assert self.type_registry_preset_relay, "The 'TYPE_REGISTRY_PRESET_RELAY' parameter is not provided"
        logger.info("[ENV] 'TYPE_REGISTRY_PRESET_RELAY': %s", self.type_registry_preset_relay)

        logger.info("[ENV] Get 'ERA_DURATION_IN_BLOCKS'")
        era_duration_in_blocks = os.getenv('ERA_DURATION_IN_BLOCKS')
        assert era_duration_in_blocks, "The 'ERA_DURATION_IN_BLOCKS' parameter is not provided"
        self.era_duration_in_blocks = int(era_duration_in_blocks)
        assert self.era_duration_in_blocks > 0, "The 'ERA_DURATION_IN_BLOCKS' parameter must be a positive integer"
        logger.info("[ENV] 'ERA_DURATION_IN_BLOCKS': %s", self.era_duration_in_blocks)

        logger.info("[ENV] Get 'ERAS_PER_DAY'")
        eras_per_day = os.getenv('ERAS_PER_DAY')
        assert eras_per_day, "The 'ERAS_PER_DAY' parameter is not provided"
        self.eras_per_day = int(eras_per_day)
        assert self.eras_per_day > 0, "The 'ERAS_PER_DAY' parameter must be a positive integer"
        logger.info("[ENV] 'ERAS_PER_DAY': %s", self.eras_per_day)

        logger.info("[ENV] Get 'DATABASE_QUERY_LIMIT'")
        self.database_query_limit = int(os.getenv('DATABASE_QUERY_LIMIT', DEFAULT_DATABASE_QUERY_LIMIT))
        assert self.database_query_limit > 0, "The 'DATABASE_QUERY_LIMIT' parameter must be a positive integer"
        logger.info("[ENV] 'DATABASE_QUERY_LIMIT': %s", self.database_query_limit)

        logger.info("[ENV] Get 'INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER'")
        initial_block_num_aggregated_metrics = os.getenv('INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER')
        assert initial_block_num_aggregated_metrics, \
            "The 'INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER' parameter is not provided"
        self.initial_block_num_aggregated_metrics = int(initial_block_num_aggregated_metrics)
        assert self.initial_block_num_aggregated_metrics >= 0, \
            "The 'INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER' parameter must be a non-negative integer"
        logger.info("[ENV] 'INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER': %s", self.initial_block_num_aggregated_metrics)

        logger.info("[ENV] Get 'MAX_RANGE_OF_BLOCKS'")
        self.max_range_of_blocks = int(os.getenv('MAX_RANGE_OF_BLOCKS', DEFAULT_MAX_RANGE_OF_BLOCKS))
        assert self.max_range_of_blocks > 0, "The 'MAX_RANGE_OF_BLOCKS' parameter must be a positive integer"
        logger.info("[ENV] 'MAX_RANGE_OF_BLOCKS': %s", self.max_range_of_blocks)

        logger.info("[ENV] Get 'TIMEOUT'")
        self.timeout = int(os.getenv('TIMEOUT', DEFAULT_TIMEOUT))
        assert self.timeout >= 0, "The 'TIMEOUT' parameter must be a non-negative integer"
        logger.info("[ENV] 'TIMEOUT': %s", self.timeout)

        self.validators_info_exporter_waiting_time = int(os.getenv(
            'VALIDATORS_INFO_EXPORTER_WAITING_TIME',
            DEFAULT_VALIDATORS_INFO_EXPORTER_WAITING_TIME,
        ))
        assert self.validators_info_exporter_waiting_time >= 0,\
            "The 'VALIDATORS_INFO_EXPORTER_WAITING_TIME' parameter must be a non-negative integer"

        logger.info("[ENV] Get 'PROMETHEUS_METRICS_PORT'")
        self.prometheus_metrics_port = int(os.getenv('PROMETHEUS_METRICS_PORT', DEFAULT_PROMETHEUS_METRICS_PORT))
        assert self.prometheus_metrics_port > 0, "The 'PROMETHEUS_METRICS_PORT' parameter must be a non-negative integer"
        logger.info("[ENV] 'PROMETHEUS_METRICS_PORT': %s", self.prometheus_metrics_port)

        logger.info("Checking the configuration parameters for the database")
        logger.info("[ENV] Get 'DATABASE_URL'")
        self.database_url = os.getenv('DATABASE_URL')
        assert self.database_url, "The 'DATABASE_URL' parameter is not provided"
        DatabaseManager.try_to_establish_connection(self.database_url)
        logger.info("[ENV] 'DATABASE_URL': successfully got")

        self.database_url_token_price_collector = os.getenv('DATABASE_URL_TOKEN_PRICE_COLLECTOR')
        if self.database_url_token_price_collector is None:
            logger.info("[ENV] 'DATABASE_URL_TOKEN_PRICE_COLLECTOR': not provided")
        else:
            DatabaseManagerTokenPriceCollector.try_to_establish_connection(self.database_url_token_price_collector)
            logger.info("[ENV] 'DATABASE_URL_TOKEN_PRICE_COLLECTOR': successfully got")
        logger.info("Configuration parameters for the database checked")

        logger.info("Creating a Web3 object for the aggregated metrics exporter")
        self.w3__aggregated_metrics = self._create_provider_forcibly(self.ws_urls_para)
        logger.info("Creating a Web3 object for the scalar metrics exporter")
        self.w3__scalar_metrics = self._create_provider_forcibly(self.ws_urls_para)
        logger.info("Creating a Web3 object for the validators info exporter")
        self.w3__validators_info = self._create_provider_forcibly(self.ws_urls_para)

        logger.info("Creating a SubstrateInterface object for the parachain for the aggregated metrics exporter")
        self.substrate_para__aggregated_metrics = self._create_interface_forcibly(
            self.ws_urls_para, self.ss58_format_para, self.type_registry_preset_para)
        logger.info("Creating a SubstrateInterface object for the parachain for the scalar metrics exporter")
        self.substrate_para__scalar_metrics = self._create_interface_forcibly(
            self.ws_urls_para, self.ss58_format_para, self.type_registry_preset_para)

        logger.info("Creating a SubstrateInterface object for the relay chain for the scalar metrics exporter")
        self.substrate_relay__scalar_metrics = self._create_interface_forcibly(
            self.ws_urls_relay, self.ss58_format_relay, self.type_registry_preset_relay)
        logger.info("Creating a SubstrateInterface object for the relay chain for the validators info exporter")
        self.substrate_relay__validators_info = self._create_interface_forcibly(
            self.ws_urls_relay, self.ss58_format_relay, self.type_registry_preset_relay)

        logger.info("[ENV] Get 'CONTROLLER_CONTRACT_ADDRESS'")
        contract_address = os.getenv('CONTROLLER_CONTRACT_ADDRESS')
        assert contract_address, "The Controller contract address is not provided"
        self.controller_address = self.w3__scalar_metrics.toChecksumAddress(contract_address)
        logger.info("[ENV] 'CONTROLLER_CONTRACT_ADDRESS': %s", self.controller_address)

        logger.info("[ENV] Get 'NIMBUS_CONTRACT_ADDRESS'")
        contract_address = os.getenv('NIMBUS_CONTRACT_ADDRESS')
        assert contract_address, "The Nimbus contract address is not provided"
        self.nimbus_address = self.w3__scalar_metrics.toChecksumAddress(contract_address)
        logger.info("ENV 'NIMBUS_CONTRACT_ADDRESS': %s", self.nimbus_address)

        logger.info("[ENV] Get 'ORACLE_MASTER_CONTRACT_ADDRESS'")
        contract_address = os.getenv('ORACLE_MASTER_CONTRACT_ADDRESS')
        assert contract_address, "OracleMaster contract address is not provided"
        self.oracle_master_address = self.w3__scalar_metrics.toChecksumAddress(contract_address)
        logger.info("[ENV] Get 'ORACLE_MASTER_CONTRACT_ADDRESS': %s", self.oracle_master_address)

        logger.info("[ENV] Get 'PAYOUT_SERVICE_ADDRESS'")
        self.payout_service_address = os.getenv('PAYOUT_SERVICE_ADDRESS')
        logger.info("[ENV] 'PAYOUT_SERVICE_ADDRESS': %s", self.payout_service_address)

        logger.info("[ENV] Get 'WITHDRAWAL_CONTRACT_ADDRESS'")
        contract_address = os.getenv('WITHDRAWAL_CONTRACT_ADDRESS')
        assert contract_address, "Withdrawal contract address is not provided"
        self.withdrawal_address = self.w3__scalar_metrics.toChecksumAddress(contract_address)
        logger.info("[ENV] 'WITHDRAWAL_CONTRACT_ADDRESS': %s", self.withdrawal_address)

        logger.info("[ENV] Get 'XCTOKEN_CONTRACT_ADDRESS'")
        contract_address = os.getenv('XCTOKEN_CONTRACT_ADDRESS')
        assert contract_address, "The xcToken contract address is not provided"
        self.xctoken_address = self.w3__scalar_metrics.toChecksumAddress(contract_address)
        logger.info("[ENV] 'XCTOKEN_CONTRACT_ADDRESS': %s", self.xctoken_address)

        logger.info("Checking contract addresses")
        self.check_contract_addresses(
            self.nimbus_address,
            self.oracle_master_address,
            self.withdrawal_address,
            self.xctoken_address,
        )
        logger.info("Contract addresses checked")

        logger.info("Checking contract ABIs")
        self.ledger_abi = self.get_abi(ledger_abi_path)

        self.nimbus_abi = self.get_abi(nimbus_abi_path)
        ABIChecker.check_nimbus_contract_abi(self.w3__scalar_metrics, self.nimbus_address, self.nimbus_abi)

        self.oracle_master_abi = self.get_abi(oracle_master_abi_path)
        ABIChecker.check_oracle_master_contract_abi(
            self.w3__scalar_metrics,
            self.oracle_master_address,
            self.oracle_master_abi,
        )

        self.withdrawal_abi = self.get_abi(withdrawal_abi_path)
        ABIChecker.check_withdrawal_contract_abi(self.w3__scalar_metrics, self.withdrawal_address, self.withdrawal_abi)

        self.xctoken_abi = self.get_abi(xctoken_abi_path)
        ABIChecker.check_xctoken_contract_abi(self.w3__scalar_metrics, self.xctoken_address, self.xctoken_abi)
        logger.info("Contract ABIs checked")

        logger.info("Successfully checked configuration parameters")

    def _create_provider_forcibly(self, ws_urls: List[str]) -> Web3:
        """Force attempt to create a Web3 object."""
        for _ in range(MAX_ATTEMPTS_TO_RECONNECT):
            try:
                w3 = utils.create_provider(ws_urls, self.timeout)
            except utils.EXPECTED_NETWORK_EXCEPTIONS as exc:
                logger.warning("Error: %s - %s", type(exc), exc)
            else:
                return w3

        sys.exit("Failed to create a Web3 object")

    def _create_interface_forcibly(self, ws_urls: list, ss58_format: int, type_registry_preset: str) -> SubstrateInterface:
        """Force attempt to create a SubstrateInterface object."""
        for _ in range(MAX_ATTEMPTS_TO_RECONNECT):
            try:
                substrate = utils.create_interface(
                    urls=ws_urls,
                    ss58_format=ss58_format,
                    type_registry_preset=type_registry_preset,
                    timeout=self.timeout,
                )
            except utils.EXPECTED_NETWORK_EXCEPTIONS as exc:
                logger.warning("Error: %s - %s", type(exc), exc)
            else:
                return substrate

        sys.exit("Failed to create a SubstrateInterface object")

    def check_contract_addresses(self, *contract_addresses: str):
        """Check whether the correct contract address is provided."""
        for contract_address in contract_addresses:
            if contract_address is None:
                continue
            logger.info("Checking the address %s", contract_address)

            contract_code = self.w3__scalar_metrics.eth.get_code(Web3.toChecksumAddress(contract_address))
            if len(contract_code) < 3:
                raise ValueError(f"Incorrect contract address or the contract is not deployed: {contract_address}")

    @staticmethod
    def get_abi(abi_path: str) -> list:
        """Get the ABI from file."""
        with open(abi_path, 'r', encoding='UTF-8') as file:
            return json.load(file)

    @staticmethod
    def _check_log_level(log_level: str):
        """Check the logger level based on the default list."""
        log_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        if log_level not in log_levels:
            raise ValueError(f"Valid `LOG_LEVEL` values: {log_levels}")

    @staticmethod
    def is_valid_urls(urls: List[str]) -> bool:
        """Check if invalid urls are in the list"""
        for url in urls:
            parsed_url = urllib.parse.urlparse(url)
            try:
                assert parsed_url.scheme in ("ws", "wss")
                assert parsed_url.params == ""
                assert parsed_url.fragment == ""
                assert parsed_url.hostname is not None
            except AssertionError:
                return False

        return True
