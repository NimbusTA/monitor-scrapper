"""This module contains the MetricsExporter class."""
import os

from prometheus_client import Gauge


class MetricsExporter:
    """This class contains different Prometheus metrics which are exposed by the service's threads."""
    def __init__(self, _prefix: str):
        if _prefix is None:
            _prefix = ''

        # NOTE: Ledger (the contract) metrics in the parachain
        self.parachain_ledger_active_balance = Gauge(
            documentation="The active balance of the ledger in the parachain",
            labelnames=['address'],
            name='parachain_ledger_active_balance',
            namespace=_prefix,
        )
        self.parachain_ledger_borrow = Gauge(
            documentation="nimbus.ledgerBorrow(ledger_i)",
            labelnames=['address'],
            name='parachain_ledger_borrow',
            namespace=_prefix,
        )
        self.parachain_ledger_cached_total_balance = Gauge(
            documentation="The balance of the ledger at the time of the last report",
            labelnames=['address'],
            name='parachain_ledger_cached_total_balance',
            namespace=_prefix,
        )
        self.parachain_ledger_locked_balance = Gauge(
            documentation="The locked balance of the ledger",
            labelnames=['address'],
            name='parachain_ledger_locked_balance',
            namespace=_prefix,
        )
        self.parachain_ledger_status = Gauge(
            documentation="The status of the ledger. 0 - Idle, 1 - Nominator, 2 - Validator, 3 - None.",
            labelnames=['address'],
            name='parachain_ledger_status',
            namespace=_prefix,
        )
        self.parachain_ledger_total_balance = Gauge(
            documentation="The total balance of the ledger",
            labelnames=['address'],
            name='parachain_ledger_total_balance',
            namespace=_prefix,
        )
        self.parachain_ledger_transfer_downward_balance = Gauge(
            name='parachain_ledger_transfer_downward_balance',
            documentation="The amount of transferred tokens in the parachain",
            labelnames=['address'],
            namespace=_prefix,
        )
        self.parachain_ledger_transfer_upward_balance = Gauge(
            name='parachain_ledger_transfer_upward_balance',
            documentation="The amount of transferred tokens in the relay chain",
            labelnames=['address'],
            namespace=_prefix,
        )
        self.parachain_ledger_stake = Gauge(
            name='parachain_ledger_stake',
            documentation="nimbus.ledgerStake()",
            labelnames=['address'],
            namespace=_prefix,
        )
        self.parachain_ledgers_stake = Gauge(
            documentation="The sum of ledger stakes",
            name='parachain_ledgers_stake',
            namespace=_prefix,
        )
        self.parachain_ledger_xctoken_balance = Gauge(
            documentation="xcToken.balanceOf(ledger[i])",
            labelnames=['address'],
            name='parachain_ledger_xctoken_balance',
            namespace=_prefix,
        )

        # NOTE: Ledger (the contract) metrics in the relay chain
        self.relay_chain_ledger_earliest_era_for_unlocking = Gauge(
            documentation="The earliest era for unlocking for the ledger",
            labelnames=['address'],
            name='relay_chain_ledger_earliest_era_for_unlocking',
            namespace=_prefix,
        )
        self.relay_chain_ledger_stake_status = Gauge(
            documentation="The stake status of the ledger",
            labelnames=['address'],
            name='relay_chain_ledger_stake_status',
            namespace=_prefix,
        )
        self.relay_chain_ledger_active_balance = Gauge(
            documentation="The active balance of the ledger",
            labelnames=['address'],
            name='relay_chain_ledger_active_balance',
            namespace=_prefix,
        )
        self.relay_chain_ledger_stash_balance = Gauge(
            documentation="The stash balance of the ledger",
            labelnames=['address'],
            name='relay_chain_ledger_stash_balance',
            namespace=_prefix,
        )
        self.relay_chain_ledger_total_balance = Gauge(
            documentation="The total balance of the ledger",
            labelnames=['address'],
            name='relay_chain_ledger_total_balance',
            namespace=_prefix,
        )
        self.relay_chain_ledger_total_unlocking_balance = Gauge(
            name='relay_chain_ledger_total_unlocking_balance',
            documentation="The total unlocking balance of the ledger",
            labelnames=['address'],
            namespace=_prefix,
        )
        self.relay_chain_ledger_validators_count = Gauge(
            documentation="The number of nominated validators",
            labelnames=['ledger'],
            name='validators_count',
            namespace=_prefix,
        )

        # NOTE: Nimbus metrics
        self.nimbus_buffered_deposits = Gauge(
            documentation="Nimbus bufferedDeposits",
            name='nimbus_bufferedDeposits',
            namespace=_prefix,
        )
        self.nimbus_buffered_redeems = Gauge(
            documentation="Nimbus bufferedRedeems",
            name='nimbus_bufferedRedeems',
            namespace=_prefix,
        )
        self.nimbus_tokens = Gauge(
            documentation="Amount of tokens on the Nimbus balance",
            name='nimbus_tokens',
            namespace=_prefix,
        )
        self.nimbus_total_supply = Gauge(
            documentation="The total supply of liquid tokens issued",
            name='nimbus_total_supply',
            namespace=_prefix,
        )
        self.nimbus_total_supply_usd = Gauge(
            documentation="The total supply in USD of liquid tokens issued",
            name='nimbus_total_supply_usd',
            namespace=_prefix,
        )
        self.nimbus_deposited_events_number = Gauge(
            documentation="The number of the Deposited event calls",
            name='nimbus_deposited_events_number',
            namespace=_prefix,
        )
        self.nimbus_redeemed_events_number = Gauge(
            documentation="The number of the Redeemed event calls",
            name='nimbus_redeemed_events_number',
            namespace=_prefix,
        )
        self.nimbus_deposited = Gauge(
            name='nimbus_deposits',
            documentation="The amount of Deposited xcToken",
            namespace=_prefix,
        )
        self.nimbus_redeemed = Gauge(
            documentation="The amount of redeemed nTOKEN",
            name='nimbus_redeems',
            namespace=_prefix,
        )
        self.nimbus_losses_aggregated = Gauge(
            documentation="Aggregated values of Losses events for all blocks",
            name='nimbus_losses_aggregated',
            namespace=_prefix,
        )
        self.nimbus_rewards_aggregated = Gauge(
            documentation="The aggregated value of the Rewards event for all blocks",
            name='nimbus_rewards_aggregated',
            namespace=_prefix,
        )
        self.holders_number = Gauge(
            documentation="The number of holders in Nimbus",
            name='holders_number',
            namespace=_prefix,
        )

        # NOTE: Withdrawal metrics
        self.withdrawal_tokens = Gauge(
            documentation="xcToken.balanceOf(Withdrawal)",
            name='withdrawal_tokens',
            namespace=_prefix,
        )
        self.withdrawal_pending_for_claiming = Gauge(
            documentation="Withdrawal.pendingForClaiming()",
            name='withdrawal_pending_for_claiming',
            namespace=_prefix,
        )
        self.withdrawal_total_virtual_xctoken_amount = Gauge(
            documentation="Withdrawal.totalVirtualXcKSMAmount()",
            name='withdrawal_total_virtual_xctoken_amount',
            namespace=_prefix,
        )
        self.withdrawal_total_xctoken_pool_shares = Gauge(
            documentation="Withdrawal.totalXcKSMPoolShares()",
            name='withdrawal_total_xctoken_pool_shares',
            namespace=_prefix,
        )

        # NOTE: Oracle (the service) and OracleMaster (the contract) metrics
        self.oracle_master_current_era_id = Gauge(
            name='oracle_master_getCurrentEraId',
            documentation="The result of calling the getCurrentEraId method",
            namespace=_prefix,
        )
        self.oracle_master_era_id = Gauge(
            documentation="The eraId value",
            name='oracle_master_eraId',
            namespace=_prefix,
        )
        self.oracle_master_next_era_start_time = Gauge(
            documentation="The start time of the next era in the OracleMaster contract",
            name='oracle_master_next_era_start_time',
            namespace=_prefix,
        )
        self.oracle_service_balance = Gauge(
            documentation="The balance of the Oracle's account",
            labelnames=['address'],
            name='oracle_service_balance',
            namespace=_prefix,
        )

        # NOTE: General parachain metrics
        self.parachain_block_number = Gauge(
            documentation="The number of the last processed block in the parachain",
            name='parachain_block_number',
            namespace=_prefix,
        )
        self.parachain_last_block_number_with_events = Gauge(
            documentation="The number of the last block in which at least one of the following events occurred:"
                          " Transfer, Rewards, Losses, Deposited or Redeemed",
            name='parachain_last_block_number_with_events',
            namespace=_prefix,
        )

        # NOTE: General relay chain metrics
        self.relay_chain_active_era_id = Gauge(
            documentation="The active era id in the relay chain",
            name='relay_chain_active_era_id',
            namespace=_prefix,
        )
        self.relay_chain_total_staked_tokens = Gauge(
            documentation="The total staked amount of tokens in the relay chain",
            name='relay_chain_total_staked_tokens',
            namespace=_prefix,
        )
        self.relay_chain_next_era_start_time = Gauge(
            'relay_chain_next_era_start_time',
            "The start time of the next era in the relay chain",
            namespace=_prefix,
        )

        # NOTE: APR and other metrics, related to that
        self.apr = Gauge(
            documentation="APR calculations based on the Nimbus contract events",
            name='APR',
            namespace=_prefix,
        )
        self.apr_per_month = Gauge(
            documentation="APR per month",
            name='APR_per_month',
            namespace=_prefix,
        )
        self.apr_per_week = Gauge(
            documentation="APR per week",
            name='APR_per_week',
            namespace=_prefix,
        )
        self.inflation_rate = Gauge(
            documentation="Current inflation rate according to the Polkadot algorithm",
            name='inflation_rate',
            namespace=_prefix,
        )

        # NOTE: Balances metrics
        self.controller_balance = Gauge(
            name='controller_balance',
            documentation="The balance of the Controller contract",
            namespace=_prefix,
        )
        self.payout_service_balance = Gauge(
            name='payout_service_balance',
            documentation="The balance of the Payout service",
            labelnames=['address'],
            namespace=_prefix,
        )

        # NOTE: metrics that are used for alerting
        self.alert_not_connected_aggregated_metrics_exporter = Gauge(
            name='alert_not_connected_aggregated_metrics_exporter',
            documentation="The AggregatedMetricsExporter is not connected to chains",
            namespace=_prefix,
        )
        self.alert_not_connected_scalar_metrics_exporter = Gauge(
            name='alert_not_connected_scalar_metrics_exporter',
            documentation="The ScalarMetricsExporter is not connected to chains",
            namespace=_prefix,
        )
        self.alert_not_connected_validators_info_exporter = Gauge(
            name='alert_not_connected_validators_info_exporter',
            documentation="The ValidatorsInfoExporter is not connected to chains",
            namespace=_prefix,
        )
        self.alert_thread_is_failed = Gauge(
            name="alert_thread_is_failed",
            documentation="One of the threads is failed",
            labelnames=['thread'],
            namespace=_prefix,
        )
        self.alert_token_price_is_none = Gauge(
            name="alert_token_price_is_none",
            documentation="Failed to get the token price from the TokenPriceCollector database",
            namespace=_prefix,
        )


prefix = os.getenv('PROMETHEUS_METRICS_PREFIX', '')
metrics_exporter = MetricsExporter(prefix)
