"""This module contains a set of methods of (re-)creating Substrate interfaces and web3 providers, calculating APR and
distributing events by blocks."""
import logging
import time

from socket import gaierror
from typing import List

from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from web3 import Web3
from websockets.exceptions import ConnectionClosedError, InvalidMessage, InvalidStatusCode


logger = logging.getLogger(__name__)

DAYS_IN_YEAR = 365
N_ENTRIES_TO_REMOVE = 0


EXPECTED_NETWORK_EXCEPTIONS = (
    BrokenPipeError,
    ConnectionClosedError,
    ConnectionRefusedError,
    ConnectionResetError,
    gaierror,
    InvalidMessage,
    InvalidStatusCode,
    SubstrateRequestException,
    TimeoutError,
)


def restore_connection_to_parachain(w3: Web3, ws_urls: List[str], timeout: int,
                                    substrate: SubstrateInterface = None) -> (SubstrateInterface, Web3):
    """Restore the connection to the parachain."""
    logger.info("Reconnecting to the parachain")
    while True:
        try:
            if substrate:
                substrate.websocket.shutdown()
                substrate = create_interface(
                    urls=ws_urls,
                    ss58_format=substrate.ss58_format,
                    type_registry_preset=substrate.type_registry_preset,
                    timeout=timeout,
                    substrate=substrate,
                )
            w3 = create_provider(timeout=timeout, urls=ws_urls, w3=w3)
            assert w3.isConnected()
            break
        except Exception as exc:
            exc_type = type(exc)
            if exc_type in EXPECTED_NETWORK_EXCEPTIONS:
                logger.warning("[para] An exception occurred: %s - %s", exc_type, exc)
            else:
                logger.error("[para] An exception occurred: %s - %s", exc_type, exc)

    return substrate, w3


def restore_connection_to_relay_chain(ws_urls: List[str], timeout: int, substrate: SubstrateInterface) -> SubstrateInterface:
    """Restore the connection to the relay chain."""
    logger.info("Reconnecting to the relay chain")
    while True:
        try:
            substrate.websocket.shutdown()
            substrate = create_interface(
                urls=ws_urls,
                ss58_format=substrate.ss58_format,
                type_registry_preset=substrate.type_registry_preset,
                timeout=timeout,
                substrate=substrate,
            )
            break
        except Exception as exc:
            exc_type = type(exc)
            if exc_type in EXPECTED_NETWORK_EXCEPTIONS:
                logger.warning("[para] An exception occurred: %s - %s", exc_type, exc)
            else:
                logger.error("[para] An exception occurred: %s - %s", exc_type, exc)

    return substrate


def create_provider(urls: List[str], timeout: int = 60, w3: Web3 = None) -> Web3:
    """Create the web3 websocket provider with one of the nodes given in the list."""
    while True:
        for url in urls:
            try:
                if w3 is None or not w3.isConnected():
                    provider = Web3.WebsocketProvider(url)
                    w3 = Web3(provider)
                else:
                    logger.info("[web3py] Not recreating a provider: the connection is already established to %s", url)
                if not w3.isConnected():
                    raise ConnectionRefusedError
                logger.info("[web3py] Successfully connected to %s", url)

                return w3
            except RuntimeError:
                w3 = None
            except Exception as exc:
                logger.warning("[web3py] Failed to connect to the node: %s - %s", type(exc), exc)

        logger.error("[web3py] Failed to connect to any node")
        logger.info("Timeout: %s seconds", timeout)
        time.sleep(timeout)


def create_interface(
        urls: List[str],
        ss58_format: int,
        type_registry_preset: str,
        timeout: int = 60,
        substrate: SubstrateInterface = None,
) -> SubstrateInterface:
    """Create a Substrate interface with one of the nodes given in the list."""
    while True:
        for url in urls:
            try:
                if substrate:
                    substrate.websocket.shutdown()
                    substrate.websocket.connect(url)
                else:
                    substrate = SubstrateInterface(
                            url=url,
                            ss58_format=ss58_format,
                            type_registry_preset=type_registry_preset,
                        )
                    substrate.update_type_registry_presets()

            except Exception as exc:
                logger.warning("[substrateinterface] Failed to connect to %s: %s", url, exc)
                if isinstance(exc.args[0], str) and exc.args[0].find("Unsupported type registry preset") != -1:
                    raise ValueError(exc.args[0]) from exc

            else:
                logger.info("[substrateinterface] The connection was made at the address: %s", url)
                return substrate

        logger.error("[substrateinterface] Failed to connect to any node")
        logger.info("Timeout: %s seconds", timeout)
        time.sleep(timeout)


def distribute_events_by_blocks(events: dict) -> dict:
    """Distribute events by blocks."""
    distributed_events = {}
    for key in events.keys():
        for event in events[key]:
            block_number = event.get('blockNumber')

            if block_number not in distributed_events:
                distributed_events[block_number] = {}
                for k in events.keys():
                    distributed_events[block_number][k] = []

            distributed_events[block_number][key].append(event)

    return distributed_events


def calculate_apr(total_apr: float, rewards: dict, eras_per_day: int, apr_min: float, apr_max: float) -> float:
    """Calculate APR."""
    apr = {}
    averaged_apr = {}

    for ledger in rewards.keys():
        apr[ledger] = []
        for reward in rewards[ledger]:
            denominator = reward[2] - reward[1]
            _apr = reward[1] / denominator * eras_per_day * DAYS_IN_YEAR if denominator else 0
            if _apr < apr_min or _apr > apr_max:
                continue
            apr[ledger].append(_apr)

    for ledger, value in apr.items():
        if not value:
            continue
        averaged_apr[ledger] = sum(value) / len(value)

    if averaged_apr:
        total_apr = sum(averaged_apr.values()) / len(averaged_apr)

    return float(total_apr)


def remove_redundant_rewards_entries(rewards: list, rewards_number: int, preset_limit: int = None):
    """Remove the last N entries from the tuple which are ordered by block (descending)"""
    if not rewards or N_ENTRIES_TO_REMOVE == 0:
        return

    rewards_current_number = len(rewards)
    if rewards_current_number <= rewards_number:
        del rewards[-N_ENTRIES_TO_REMOVE:]
        return

    if preset_limit is None:
        return

    diff = rewards_number - preset_limit
    if diff > N_ENTRIES_TO_REMOVE:
        return

    del rewards[-(N_ENTRIES_TO_REMOVE - diff):]


def decrease_range_of_blocks(range_current: int) -> int:
    """Deduct (coefficient * 100) percents from the range of blocks """
    coefficient = 0.1  # 10%
    range_new = max(range_current - int(range_current * coefficient), 1)
    if range_new == range_current and range_new > 1:
        range_new -= 1
    logger.info("Deduct %s percents from the range of blocks. The new range is %s", coefficient * 100, range_new)

    return range_new
