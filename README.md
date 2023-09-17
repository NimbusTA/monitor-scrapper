# nimbus-monitor-scraper
Indexing and monitoring tool for the Nimbus protocol.


## Requirements
* Python 3.9


## Setup
```shell
pip install -r requirements.txt
```


## Run
The service receives its configuration parameters from environment variables. Export required parameters from the list below and start the service:
```shell
bash run.sh
```

To stop the service, send the SIGINT or SIGTERM signal to the process.


## List of functions, events and states from contracts that are used by the service
#### Ledger
States:
1) `activeBalance`
2) `cachedTotalBalance`
3) `lockedBalance`
4) `status`
5) `totalBalance`
6) `transferDownwardBalance`
7) `transferUpwardBalance`

#### Nimbus
Functions and states:
1) `balanceOf`
2) `bufferedDeposits`
3) `bufferedRedeems`
4) `findLedger`
5) `getLedgerAddresses`
6) `getPooledTokenByShares`
7) `getStashAccounts`
8) `ledgerBorrow`
9) `ledgerStake`
10) `totalSupply`

Events:
1) `Deposited`
2) `Losses`
3) `Redeemed`
4) `Rewards`
5) `Transfer`

#### OracleMaster
Functions and states:
1) `ANCHOR_ERA_ID`
2) `ANCHOR_TIMESTAMP`
3) `eraId`
4) `getCurrentEraId`
5) `getStashAccounts`
6) `MAX_MEMBERS`
7) `members`
8) `SECONDS_PER_ERA`

#### Withdrawal
States:
1) `pendingForClaiming`
2) `totalVirtualXcTokenAmount`
3) `totalXcTokenPoolShares`

#### xcTOKEN
Functions:
1) `balanceOf`


## Configuration parameters
#### Required
* `DATABASE_URL` - The URL to the monitor-scraper database. Example: `postgres://admin:1234@localhost:5432/monitor-scraper`.
* `ERA_DURATION_IN_BLOCKS` - The duration of era in blocks.
* `ERAS_PER_DAY` - The number of eras per day. The default value is `1`.
* `INITIAL_BLOCK_NUMBER_AGGREGATED_METRICS_EXPORTER` - The number of the block, from which the AggregatedMetricsExporter starts scraping if the database is not found.
* `TYPE_REGISTRY_PRESET_PARA`
* `TYPE_REGISTRY_PRESET_RELAY`
* `WS_URLS_PARA` - WS URLs of the parachain nodes. **Must be comma-separated**, example: `ws://localhost:10059/,ws://localhost:10055/`.
* `WS_URLS_RELAY` - WS URLs of the relay chain nodes. **Must be comma-separated**, example: `ws://localhost:9959/,ws://localhost:9957/`.

##### The inflation rate calculation
* `STAKE_TARGET`
* `AUCTION_MAX`
* `AUCTION_ADJUST`
* `MAX_INFLATION`
* `MIN_INFLATION`
* `FALLOFF`

##### Contract addresses (example: `0x000000000000000000000000000000000000dEaD`)
* `CONTROLLER_CONTRACT_ADDRESS` - The address of the Controller contract.
* `NIMBUS_CONTRACT_ADDRESS` - The address of the Nimbus contract.
* `ORACLE_MASTER_CONTRACT_ADDRESS` - The address of the OracleMaster contract.
* `WITHDRAWAL_CONTRACT_ADDRESS` - The address of the Withdrawal contract.
* `XCTOKEN_CONTRACT_ADDRESS` - The address of the xcToken contract.


#### Optional
* `DATABASE_QUERY_LIMIT` - The maximum number of rows to be extracted from the database for the APR calculation for each ledger. The default value is `100`.
* `LOG_LEVEL` - The logging level of the logging module: `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`. The default level is `INFO`.
* `MAX_RANGE_OF_BLOCKS` - The maximum number of blocks that can be extracted in one query from the chain. The default value is `2500`.
* `PAYOUT_SERVICE_ADDRESS` - The address of the Payout service.
* `PROMETHEUS_METRICS_PORT` - The port of the Prometheus HTTP server. The default value is `8000`.
* `PROMETHEUS_METRICS_PREFIX` - The prefix for Prometheus metrics. The default value is ``.
* `SS58_FORMAT_PARA` - The default value is `42`.
* `SS58_FORMAT_RELAY` - The default value is `42`.
* `TIMEOUT` - The time (in seconds) the failure node stays in the black list while a thread is trying to reconnect. The default value is `60`.
* `VALIDATORS_INFO_EXPORTER_WAITING_TIME` - The time of waiting (in seconds) between validators info updates. The default value is `60`.

##### Paths to ABIs
* `LEDGER_CONTRACT_ABI_PATH` - The path to the Ledger ABI file. The default value is `./assets/Ledger.json`.
* `NIMBUS_CONTRACT_ABI_PATH` - The path to the Nimbus ABI file. The default value is `./assets/Nimbus.json`.`.
* `ORACLE_MASTER_CONTRACT_ABI_PATH` - The path to the OracleMaster ABI file. The default value is `./assets/OracleMaster.json`.
* `WITHDRAWAL_CONTRACT_ABI_PATH` - The path to the Withdrawal ABI file. The default value is `./assets/Withdrawal.json`.
* `XCTOKEN_CONTRACT_ABI_PATH` - The path to the xcToken ABI file. The default value is `./assets/xcToken.json`.

##### APR
* `APR` - The value to display as a Prometheus metric. If omitted, the actual APR is displayed.
* `APR_MIN` - The lower threshold of the APR value (in %). The default value is `3`.
* `APR_MAX` - The upper threshold of the APR value (in %). The default value is `45`.

* `DATABASE_URL_TOKEN_PRICE_COLLECTOR` - The URL to the token-price-collector database. Example: `postgres://admin:1234@localhost:5432/token-price-collector`.
* `TOKEN_PRICE_COLLECTOR_TOKEN_SYMBOL` - The symbol of the token for the TokenPriceCollector database.


## Prometheus metrics

Prometheus exporter provides the following metrics.

| name                                                               | description                                                                                                                           | frequency                                         |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| **APR**                                               <br> *Gauge* | APR calculations based on the Nimbus contract events                                                                                  | Every parachain block that has at least one event |
| **APR_per_month**                                     <br> *Gauge* | APR per month                                                                                                                         | Every relay chain block                           |
| **controller_balance**                                <br> *Gauge* | The balance of the Controller contract                                                                                                | Every parachain block                             |
| **holders_number**                                    <br> *Gauge* | The number of holders in Nimbus                                                                                                       | Every block per each Transfer event               |
| **inflation_rate**                                    <br> *Gauge* | The current inflation rate according to the Polkadot algorithm                                                                        | Every relay chain block                           |
| **nimbus_buffered_deposits**                          <br> *Gauge* | Haven't executed buffered deposits                                                                                                    | Every parachain block                             |
| **nimbus_buffered_redeems**                           <br> *Gauge* | Haven't executed buffered redeems                                                                                                     | Every parachain block                             |
| **nimbus_deposited_events_number**                    <br> *Gauge* | The number of the Deposited event calls                                                                                               | Every Deposited event                             |
| **nimbus_deposited**                                  <br> *Gauge* | The amount of deposited xcToken                                                                                                       | Every Deposited event                             |
| **nimbus_losses_aggregated**                          <br> *Gauge* | Aggregated values of Losses events for all blocks                                                                                     | Every Losses event                                |
| **nimbus_redeemed_events_number**                     <br> *Gauge* | The number of the Redeemed event calls                                                                                                | Every Redeemed event                              |
| **nimbus_redeemed**                                   <br> *Gauge* | The amount of redeemed xcToken                                                                                                        | Every Deposited event                             |
| **nimbus_rewards_aggregated**                         <br> *Gauge* | The aggregated value of the Rewards event for all blocks                                                                              | Every Rewards event                               |
| **nimbus_tokens**                                     <br> *Gauge* | Amount of tokens on the Nimbus balance                                                                                                | Every parachain block                             |
| **nimbus_total_supply**                               <br> *Gauge* | The total supply of liquid tokens issued                                                                                              | Every parachain block                             |
| **nimbus_total_supply_usd**                           <br> *Gauge* | The total supply in USD of liquid tokens issued                                                                                       | Every parachain block                             |
| **oracle_master_next_era_start_time**                 <br> *Gauge* | The start time of the next era in the OracleMaster contract                                                                           | Every parachain block                             |
| **oracle_service_balance**                            <br> *Gauge* | The balance of the Oracle's account                                                                                                   | Every parachain block                             |
| **oracle_master_getCurrentEraId**                     <br> *Gauge* | The result of calling the getCurrentEraId method                                                                                      | Every parachain block                             |
| **oracle_master_eraId**                               <br> *Gauge* | The eraId value from the OracleMaster contract                                                                                        | Every parachain block                             |
| **parachain_block_number**                            <br> *Gauge* | The number of the last processed block in the parachain                                                                               | Every second                                      |
| **parachain_last_block_number_with_events**           <br> *Gauge* | The number of the last block in which at least one of the following events occurred: Transfer, Rewards, Losses, Deposited or Redeemed | Every block that has at least one event           |
| **parachain_ledger_active_balance**                   <br> *Gauge* | The active balance of the ledger in the parachain                                                                                     | Every parachain block                             |
| **parachain_ledger_borrow**                           <br> *Gauge* | nimbus.ledgerBorrow(ledger_i)                                                                                                         | Every parachain block                             |
| **parachain_ledger_cached_total_balance**             <br> *Gauge* | The balance of the ledger at the time of the last report                                                                              | Every parachain block                             |
| **parachain_ledger_locked_balance**                   <br> *Gauge* | The locked balance of the ledger                                                                                                      | Every parachain block                             |
| **parachain_ledger_status**                           <br> *Gauge* | The status of the ledger. 0 - Idle, 1 - Nominator, 2 - Validator, 3 - None.                                                           | Every relay chain block                           |
| **parachain_ledger_total_balance**                    <br> *Gauge* | The total balance of the ledger                                                                                                       | Every parachain block                             |
| **parachain_ledger_transfer_downward_balance**        <br> *Gauge* | The amount of transferred tokens in the parachain                                                                                     | Every parachain block                             |
| **parachain_ledger_transfer_upward_balance**          <br> *Gauge* | The amount of transferred tokens in the relay chain                                                                                   | Every parachain block                             |
| **parachain_ledger_stake**                            <br> *Gauge* | nimbus.ledgerStake()                                                                                                                  | Every parachain block                             |
| **parachain_ledger_xctoken_balance**                  <br> *Gauge* | xcToken.balanceOf(ledger_i))                                                                                                          | Every parachain block                             |
| **parachain_ledgers_stake**                           <br> *Gauge* | Sum(nimbus.ledgerStake(ledger_i))                                                                                                     | Every parachain block                             |
| **payout_service_balance**                            <br> *Gauge* | The balance of the Payout service                                                                                                     | Every relay chain block                           |
| **relay_chain_active_era_id**                         <br> *Gauge* | The active era id in the relay chain                                                                                                  | Every relay chain block                           |
| **relay_chain_ledger_active_balance**                 <br> *Gauge* | The active balance of the ledger                                                                                                      | Every relay chain block                           |
| **relay_chain_ledger_earliest_era_for_unlocking**     <br> *Gauge* | The earliest era for unlocking for the ledger                                                                                         | Every relay chain block                           |
| **relay_chain_ledger_stake_status**                   <br> *Gauge* | The stake status of the ledger                                                                                                        | Every relay chain block                           |
| **relay_chain_ledger_stash_balance**                  <br> *Gauge* | The stash balance of the ledger                                                                                                       | Every relay chain block                           |
| **relay_chain_ledger_total_balance**                  <br> *Gauge* | The total balance of the ledger                                                                                                       | Every relay chain block                           |
| **relay_chain_ledger_total_unlocking_balance**        <br> *Gauge* | The total unlocking balance of the ledger                                                                                             | Every relay chain block                           |
| **relay_chain_ledger_validators_count**               <br> *Gauge* | The number of nominated validators                                                                                                    | Every relay chain block                           |
| **relay_chain_next_era_start_time**                   <br> *Gauge* | The start time of the next era in the relay chain                                                                                     | Every relay chain block                           |
| **relay_chain_total_staked_tokens**                   <br> *Gauge* | The total staked amount of tokens in relay chain                                                                                      | Every relay chain block                           |
| **withdrawal_tokens**                                 <br> *Gauge* | xcToken.balanceOf(Withdrawal)                                                                                                         | Every parachain block                             |
| **withdrawal_pending_for_claiming**                   <br> *Gauge* | Withdrawal.pendingForClaiming()                                                                                                       | Every parachain block                             |
| **withdrawal_total_virtual_xctoken_amount**           <br> *Gauge* | Withdrawal.totalVirtualXcTokenAmount()                                                                                                | Every parachain block                             |
| **withdrawal_total_xctoken_pool_shares**              <br> *Gauge* | Withdrawal.totalXcTokenPoolShares()                                                                                                   | Every parachain block                             |
