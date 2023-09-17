"""This module contains the AbiChecker class, which checks ABI of different contracts."""
import logging

from os.path import exists
from web3 import Web3
from web3.exceptions import ABIFunctionNotFound


logger = logging.getLogger(__name__)


class ABIChecker:
    """This class contains a set of methods to check ABIs of different contracts."""
    @staticmethod
    def check_abi_path(*paths: str):
        """Check paths to ABIs."""
        for path in paths:
            if not path:
                logger.warning("An empty path was found")
                continue
            if not isinstance(path, str):
                logger.warning("There is no string provided: %s. Skipping", path)
                continue

            logger.info("Checking the path: '%s'", path)
            if not exists(path):
                raise FileNotFoundError(f"The file with the ABI was not found: {path}")

    @staticmethod
    def check_nimbus_contract_abi(w3: Web3, contract_addr: str, abi: list):
        """Check the provided ABI for the Nimbus contract."""
        logger.info("Checking Nimbus ABI")
        assert contract_addr, "The contract address is not provided"

        contract = w3.eth.contract(address=w3.toChecksumAddress(contract_addr), abi=abi)
        try:
            ledger = contract.functions.getLedgerAddresses().call()[0]
            contract.functions.totalSupply().call()
            contract.functions.bufferedDeposits().call()
            contract.functions.bufferedRedeems().call()
            contract.functions.findLedger(ledger).call()
            contract.functions.ledgerBorrow(ledger).call()
            contract.functions.balanceOf(ledger).call()
            contract.functions.ledgerStake(ledger).call()
            contract.functions.getPooledTokenByShares(0).call()
        except ValueError:
            pass

        try:
            if not hasattr(contract.events, 'Transfer'):
                raise ABIFunctionNotFound("The contract does not contain the 'Transfer' event")

            if not hasattr(contract.events, 'Deposited'):
                raise ABIFunctionNotFound("The contract does not contain the 'Deposited' event")

            if not hasattr(contract.events, 'Redeemed'):
                raise ABIFunctionNotFound("The contract does not contain the 'Redeemed' event")

            if not hasattr(contract.events, 'Rewards'):
                raise ABIFunctionNotFound("The contract does not contain the 'Rewards' event")

            if not hasattr(contract.events, 'Losses'):
                raise ABIFunctionNotFound("The contract does not contain the 'Losses' event")
        except ValueError:
            pass

    @staticmethod
    def check_oracle_master_contract_abi(w3: Web3, contract_addr: str, abi: list):
        """Check the provided ABI for the OracleMaster contract."""
        logger.info("Checking OracleMaster ABI")
        assert contract_addr, "The contract address is not provided"

        contract = w3.eth.contract(address=w3.toChecksumAddress(contract_addr), abi=abi)
        try:
            contract.functions.MAX_MEMBERS().call()
            contract.functions.members(0).call()
            contract.functions.getCurrentEraId().call()
            contract.functions.ANCHOR_ERA_ID().call()
            contract.functions.SECONDS_PER_ERA().call()
            contract.functions.ANCHOR_TIMESTAMP().call()
            contract.functions.eraId().call()
            contract.functions.getStashAccounts().call()
        except ValueError:
            pass

    @staticmethod
    def check_withdrawal_contract_abi(w3: Web3, contract_addr: str, abi: list):
        """Check the provided ABI for the Withdrawal contract."""
        logger.info("Checking Withdrawal ABI")
        assert contract_addr, "The contract address is not provided"

        contract = w3.eth.contract(address=w3.toChecksumAddress(contract_addr), abi=abi)
        try:
            contract.functions.pendingForClaiming().call()
            contract.functions.totalVirtualXcTokenAmount().call()
            contract.functions.totalXcTokenPoolShares().call()
        except ValueError:
            pass

    @staticmethod
    def check_xctoken_contract_abi(w3: Web3, contract_addr: str, abi: list):
        """Check the provided ABI for the xcToken contract."""
        logger.info("Checking xcToken ABI")
        assert contract_addr, "The contract address is not provided"

        contract = w3.eth.contract(address=w3.toChecksumAddress(contract_addr), abi=abi)
        try:
            contract.functions.balanceOf(contract_addr).call()
        except ValueError:
            pass
