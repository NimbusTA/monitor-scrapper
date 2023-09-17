"""This module contains the ContractEventFilter class."""
import asyncio
import logging

from web3 import Web3


logger = logging.getLogger(__name__)


class ContractEventFilter:
    """This class contains an implementation of a filter, which extracts events from the specific contract."""
    def __init__(self, w3: Web3, contract_address: str, from_block: int, to_block: int, events_to_find: set):
        assert isinstance(from_block, int), "the 'from_block' parameter must be an integer"
        assert isinstance(to_block, int), "the 'to_block' parameter must be an integer"
        assert from_block <= to_block, "the 'from_block' is greater than the 'to_block'"

        self.topics = []
        if 'nimbus' in events_to_find:
            self.topics += [
                w3.keccak(text="Transfer(address,address,uint256)").hex(),
                w3.keccak(text="Rewards(address,uint256,uint256)").hex(),
                w3.keccak(text="Losses(address,uint256,uint256)").hex(),
                w3.keccak(text="Deposited(address,uint256)").hex(),
                w3.keccak(text="Redeemed(address,uint256)").hex(),
            ]
        # if 'swap' in events_to_find:
        #     self.topics += [
        #         w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex(),
        #     ]
        # if 'tokenexchange' in events_to_find:
        #     self.topics += [
        #         w3.keccak(text="AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)").hex(),
        #         w3.keccak(text="RemoveLiquidityOne(address,uint256,uint256,uint256)").hex(),
        #         w3.keccak(text="TokenExchange(address,int128,uint256,int128,uint256)").hex(),
        #     ]

        self.contract_address = contract_address
        self.from_block = from_block
        self.to_block = to_block

        self.uninstalled = False
        self.w3 = w3

        self._init_filter()

    def _init_filter(self):
        """Initialize a filter."""
        logger.debug("Initialize a filter: %s - [%s;%s]", self.contract_address, self.from_block, self.to_block)
        self.filter = self.w3.eth.filter({
            'address': self.contract_address,
            'fromBlock': self.from_block,
            'toBlock': self.to_block,
            'topics': [self.topics],
        })
        self.uninstalled = False

    def get_all_entries(self) -> list:
        """Get all entries from the filter."""
        if self.uninstalled:
            self._init_filter()

        entries = []
        for _ in range(2):
            try:
                entries = self.filter.get_all_entries()
                break
            except (asyncio.exceptions.TimeoutError, ValueError) as exc:
                if isinstance(exc, asyncio.exceptions.TimeoutError) or isinstance(exc, ValueError) and exc.args and \
                        'message' in exc.args[0] and exc.args[0]['message'] == 'query returned more than 10000 results':
                    self.uninstall()
                    raise exc from exc
                self._init_filter()

        sorted_events = sorted(entries, key=lambda event: event.get('blockNumber'))
        self.uninstall()
        if sorted_events:
            from_block = sorted_events[-1].get('blockNumber') + 1
            logger.info("Changing the start block (%s -> %s) for the filter", self.from_block, from_block)
            self.from_block = from_block
            if from_block > self.to_block:
                self.to_block = from_block

        return sorted_events

    def update_filter_range(self, from_block: int, to_block: int):
        """Update the filter range."""
        if not self.uninstalled:
            self.uninstall()
        self.from_block = from_block
        self.to_block = to_block
        self._init_filter()

    def uninstall(self):
        """Uninstall the filter."""
        self.uninstalled = True
        filter_id = self.filter.filter_id
        try:
            self.w3.eth.uninstall_filter(filter_id)
        except ValueError:
            logger.warning("Failed to uninstall filter while getting events: %s", filter_id)
