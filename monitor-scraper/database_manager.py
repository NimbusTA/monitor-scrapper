"""This module contains the DatabaseManager class."""
from threading import Lock
from typing import List, Tuple

import logging
import psycopg2


logger = logging.getLogger(__name__)

EXPECTED_POSTGRES_EXCEPTIONS = (
    psycopg2.errors.DuplicateObject,
    psycopg2.errors.DuplicateTable,
    psycopg2.errors.InFailedSqlTransaction,
    psycopg2.errors.UniqueViolation,
)


class DatabaseManager:
    """This class contains methods to make queries to the PostgreSQL database."""
    conn: psycopg2.extensions.connection
    lock: Lock

    def __init__(self, database_url: str):
        self.conn = psycopg2.connect(dsn=database_url)
        self.lock = Lock()

        self._create_utility_objects()
        self._create_tables_for_api()
        self._create_tables_for_aggregated_metrics_exporter()
        # self._create_tables_for_trading_volume_exporter(pools)

    def _create_utility_objects(self):
        """Create utility objects in the database."""
        logger.info("Creating the 'uint_256' type")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE DOMAIN uint_256 AS NUMERIC NOT NULL
                        CHECK (VALUE >= 0 AND VALUE < 2^256)
                        CHECK (SCALE(VALUE) = 0);
                    """)
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'uint_256' type already exists")
        else:
            logger.info("The 'uint_256' type is created successfully")

    def _create_tables_for_api(self):
        """Create table for the API"""
        logger.info("Creating the 'api' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE TABLE api (
                            apr FLOAT,
                            apr_per_month FLOAT,
                            apr_per_week FLOAT,
                            estimated_apy FLOAT,
                            inflation_rate FLOAT,
                            total_rewards uint_256,
                            total_losses uint_256,
                            total_staked_relay uint_256,
                            total_supply uint_256);
                    """)
                    # NOTE: the 'api' is a single-row table. To avoid redundant checks whether any entries exist
                    # inside the table, we simply add an empty row.
                    curs.execute("INSERT INTO api VALUES(0, 0, 0, 0, 0, 0, 0, 0, 0)")
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'api' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'api' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'api' table is created successfully")

    def _create_tables_for_aggregated_metrics_exporter(self):
        """Create tables for the AggregatedMetricsExporter."""
        logger.info("Creating the 'reward' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE TABLE reward (
                        ledger TEXT,
                        reward BIGINT,
                        balance uint_256,
                        block INT);
                    """)
                    curs.execute("CREATE INDEX idx_ledger ON reward (ledger);")
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'reward' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'reward' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'reward' table is created successfully")

        logger.info("Creating the 'holder' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("CREATE TABLE holder (holder TEXT PRIMARY KEY);")
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'holder' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'holder' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'holder' table is created successfully")

        logger.info("Creating the 'aggregated_data' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE TABLE aggregated_data (
                        deposited uint_256,
                        deposited_events_num INT,
                        redeemed uint_256,
                        redeemed_events_num INT,
                        last_block_number_with_events INT);
                    """)
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'aggregated_data' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'aggregated_data' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'aggregated_data' table is created successfully")

        logger.info("Creating the 'validators_info' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE TABLE validators_info (
                        active_stake uint_256,
                        ledger TEXT,
                        stash TEXT,
                        validators TEXT);
                    """)
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'validators_info' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'validators_info' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'validators_info' table is created successfully")

    def _create_tables_for_trading_volume_exporter(self, pools: List[str]):
        """Create tables for the TradingVolumeExporter."""
        for table_name in pools:
            logger.info("Creating the 'swap_events_%s' table", table_name)
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute(f"""
                            CREATE TABLE swap_events_{table_name} (
                            tx_hash TEXT,
                            log_index INT,
                            block_number INT,
                            block_timestamp bigint,
                            sender TEXT,
                            amount0In uint_256,
                            amount1In uint_256,
                            amount0Out uint_256,
                            amount1Out uint_256,
                            receiver TEXT,
                            token0 TEXT,
                            token1 TEXT,
                            PRIMARY KEY (tx_hash, log_index)
                        );""")
            except EXPECTED_POSTGRES_EXCEPTIONS:
                logger.info("The 'swap_events_%s' table already exists", table_name)
            except Exception as exc:
                logger.critical("Failed to create the 'swap_events_%s' table: %s - %s", table_name, type(exc), exc)
            else:
                logger.info("The 'swap_events_%s' table is created successfully", table_name)

        logger.info("Creating the 'daily_volume' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("""
                        CREATE TABLE daily_volume (id SERIAL, date TEXT, value FLOAT, pool TEXT, PRIMARY KEY (date, pool));
                    """)
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'daily_volume' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'daily_volume' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'daily_volume' table is created successfully")

        logger.info("Creating the 'swap_events_utils' table")
        try:
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute("CREATE TABLE swap_events_utils (last_processed_block_number INT, destination TEXT);")
        except EXPECTED_POSTGRES_EXCEPTIONS:
            logger.info("The 'swap_events_utils' table already exists")
        except Exception as exc:
            logger.critical("Failed to create the 'swap_events_utils' table: %s - %s", type(exc), exc)
        else:
            logger.info("The 'swap_events_utils' table is created successfully")

    @staticmethod
    def try_to_establish_connection(database_url: str):
        """Try to establish connection to the database."""
        psycopg2.connect(dsn=database_url)

    def get_holders_number(self) -> tuple:
        """Get the number of holders from the 'holder' table."""
        logger.info("Getting the number of holders")
        result = ()
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT count(holder) FROM holder;")
                        result = curs.fetchone()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get the number of holders: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully got the number of holders")

        return result

    def get_ledger_addresses(self) -> List[tuple]:
        """Get ledger addresses from the 'reward' table."""
        logger.info("Getting the addresses of ledgers")
        result = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT DISTINCT ledger FROM reward;")
                        result = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get the addresses of ledgers: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully got ledger addresses")

        return result

    def add_holder(self, holder: str):
        """Add the holder to the 'holder' table."""
        logger.info("Adding the holder: %s", holder)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("INSERT INTO holder VALUES (%s);", (holder,))
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to add the holder '%s': %s - %s", holder, type(exc), exc)
            else:
                logger.info("Successfully added the holder %s", holder)

    def get_rewards(self, ledger: str, limit: int = 100) -> List[tuple]:
        """Get the number of rewards for the specific ledger from the 'reward' table."""
        logger.info("Getting rewards of the ledger %s", ledger)
        result = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute(
                            """
                            SELECT * FROM reward
                            WHERE ledger = %s
                            ORDER BY block DESC
                            LIMIT %s;
                            """,
                            (ledger, limit)
                        )
                        result = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get rewards of the ledger '%s': %s - %s", ledger, type(exc), exc)
            else:
                logger.info("Successfully got rewards for the ledger %s", ledger)

        return result

    def get_rewards_number(self, ledger: str) -> tuple:
        """Get number of the Rewards/Losses events for a specific ledger."""
        logger.info("Getting the number of the Rewards/Losses events for the ledger %s", ledger)
        result = ()
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT count(*) FROM Reward WHERE ledger = %s", (ledger,))
                        result = curs.fetchone()
            except EXPECTED_POSTGRES_EXCEPTIONS:
                self.conn.rollback()
            except Exception as exc:
                logger.critical("Failed to get the number of rewards: %s - %s", type(exc), exc)

        return result

    def add_reward(self, ledger: str, reward: str, balance: str, block_number: int):
        """Add the reward of the specific ledger to the 'reward' table."""
        logger.info("Adding the reward %s of the ledger %s with the balance %s", reward, ledger, balance)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("INSERT INTO reward VALUES (%s, %s, %s, %s);", (ledger, reward, balance, block_number))
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to add the reward '%s' of the ledger '%s':%s - %s", reward, ledger, type(exc), exc)
            else:
                logger.info("Successfully added the reward for the ledger %s", ledger)

    def get_aggregated_data(self) -> tuple:
        """Get the aggregated data from the 'aggregated_data' table."""
        logger.info("Getting the aggregated data")
        result = ()
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT * FROM aggregated_data;")
                        result = curs.fetchone()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get the aggregated data: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully got the aggregated data")

        return result

    def update_aggregated_data(self, aggregated_data: dict):
        """Update the data in the 'aggregated_data' table."""
        logger.info("Updating the aggregated data: %s", aggregated_data)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("DELETE FROM aggregated_data;")
                        curs.execute("INSERT INTO aggregated_data VALUES (%s, %s, %s, %s, %s);", (
                            aggregated_data['deposited'],
                            aggregated_data['deposited_events_num'],
                            aggregated_data['redeemed'],
                            aggregated_data['redeemed_events_num'],
                            aggregated_data['last_processed_block_number_with_events'],
                        ))
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to update the aggregated data '%s': %s - %s", aggregated_data, type(exc), exc)
            else:
                logger.info("Successfully updated the aggregated data")

    def get_swap_events_utils(self) -> list:
        """Get entries from the 'swap_events_utils' table."""
        logger.info("Getting swap events utils")
        result = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT * FROM swap_events_utils;")
                        result = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get the swap events utils: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully got swap events utils")

        return result

    def update_swap_events_utils(self, destination: str, last_processed_block_number: int):
        """Update the 'swap_events_utils' table."""
        logger.info("Updating swap events utils")
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("DELETE FROM swap_events_utils WHERE destination = %s;", (destination,))
                        curs.execute(
                            "INSERT INTO swap_events_utils VALUES(%s, %s);",
                            (last_processed_block_number, destination)
                        )
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to update swap events utils: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully updated swap events utils")

    def get_swap_events(self, block_timestamp: int, destination: str) -> List[tuple]:
        """Get swap events from the 'swap_events_<destination>' table."""
        logger.info("Getting swap events from the 'swap_events_%s' table", destination)
        result = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute(
                            f"SELECT * FROM swap_events_{destination} WHERE block_timestamp > %s;",
                            (block_timestamp,),
                        )
                        result = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get swap events from the 'swap_events_%s': %s - %s", destination, type(exc), exc)
            else:
                logger.info("Successfully got swap events from the 'swap_events_%s' table", destination)

        return result

    def add_swap_event(self, destination: str, entry: dict):
        """Add the swap event to the 'swap_events_<destination>' table."""
        logger.info("Adding the swap event to the 'swap_events_%s' table: %s", destination, entry)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute(
                            f"""
                            INSERT INTO swap_events_{destination} (tx_hash, log_index, block_number, block_timestamp, sender,
                                                                   amount0In, amount1In, amount0Out, amount1Out, receiver,
                                                                   token0, token1)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (
                                entry['tx_hash'],
                                entry['log_index'],
                                entry['block_number'],
                                entry['block_timestamp'],
                                entry['sender'],
                                entry['amount0In'],
                                entry['amount1In'],
                                entry['amount0Out'],
                                entry['amount1Out'],
                                entry['receiver'],
                                entry['token0'],
                                entry['token1'],
                            ))
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to add the swap event, destination %s: %s - %s", destination, type(exc), exc)
            else:
                logger.info("Successfully added the swap event, destination %s: %s", destination, entry)

    def remove_swap_events(self, block_number: int, destination: List[str]):
        """Remove entries from the 'swap_events_<destination>' table where block number is greater than provided."""
        with self.lock:
            for pool in destination:
                logger.info("Removing redundant entries from the 'swap_events_%s' table, block %s", pool, block_number)
                try:
                    with self.conn:
                        with self.conn.cursor() as curs:
                            curs.execute(f"DELETE FROM swap_events_{pool} WHERE block_number > %s;", (block_number,))
                except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                    logger.info("An expected postgres exception occurred: %s", exc)
                except Exception as exc:
                    logger.error("Failed to remove redundant swap events, destination '%s': %s - %s", pool, type(exc), exc)
                else:
                    logger.info("Successfully removed redundant entries from the 'swap_events_%s' table", pool)

    def get_daily_volume_for_the_last_30_days(self, pool: str) -> List[tuple]:
        """Get daily volumes for the last 30 days."""
        logger.info("Getting daily volumes for the last 30 days for the pool %s", pool)
        result = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT value FROM daily_volume WHERE pool = %s ORDER BY id DESC LIMIT 30;", (pool,))
                        result = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get daily volumes for the last 30 days for the pool '%s': %s - %s",
                             pool, type(exc), exc)
            else:
                logger.info("Successfully got daily volumes")

        return result

    def add_daily_volume(self, date: str, value: float, pool: str):
        """Add (or update) the daily volume of the specific pool in the 'daily_volume' table."""
        logger.info("Adding (or updating) the daily volume: %s - %s - %s", date, pool, value)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute(
                            "INSERT INTO daily_volume(date, value, pool) VALUES (%s, %s, %s);",
                            (date, value, pool),
                        )
            except psycopg2.errors.UniqueViolation:
                try:
                    with self.conn:
                        with self.conn.cursor() as curs:
                            curs.execute(
                                "UPDATE daily_volume SET value = %s WHERE date = %s AND pool = %s;",
                                (value, date, pool),
                            )
                except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                    logger.info("An expected postgres exception occurred: %s", exc)
                except Exception as exc:
                    logger.error("Failed to add or update the daily volume for the '%s': %s - %s", pool, type(exc), exc)
                else:
                    logger.info("Successfully updated the daily volume for the '%s'", pool)

    def add_validators_info(self, validators_info: List[dict]):
        """Add the validators info to the 'validators_info' table."""
        logger.info("Adding the validators info: %s", validators_info)
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("DELETE FROM validators_info;")
                        for row in validators_info:
                            curs.execute("""
                                INSERT INTO validators_info (active_stake, ledger, stash, validators) VALUES (%s, %s, %s, %s);
                                """, (row['active_stake'], row['ledger'], row['stash'], row['validators'])
                            )
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to add the validators info: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully added the validators info")

    def get_all_rewards_and_losses(self) -> Tuple[List[tuple], List[tuple]]:
        """Get all rewards and losses from the 'reward' table."""
        rewards = []
        losses = []
        with self.lock:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.execute("SELECT reward FROM reward WHERE reward > 0;")
                        rewards = curs.fetchall()
                        curs.execute("SELECT reward FROM reward WHERE reward < 0;")
                        losses = curs.fetchall()
            except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                logger.info("An expected postgres exception occurred: %s", exc)
            except Exception as exc:
                logger.error("Failed to get rewards and losses: %s - %s", type(exc), exc)
            else:
                logger.info("Successfully got rewards and losses")

        return rewards, losses

    def update_api_table(self, args: dict):
        """Update the 'api' table."""
        with self.lock:
            for key, value in args.items():
                try:
                    with self.conn:
                        with self.conn.cursor() as curs:
                            curs.execute(f"UPDATE api SET {key} = %s;", (value,))
                except EXPECTED_POSTGRES_EXCEPTIONS as exc:
                    logger.info("An expected postgres exception occurred: %s", exc)
                except Exception as exc:
                    logger.error("Failed to update the api table, {'%s': %s}: %s - %s", key, value, type(exc), exc)
                else:
                    logger.info("Successfully update the api table, key %s: %s", key, value)
