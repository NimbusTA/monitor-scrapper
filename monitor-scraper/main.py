#!/usr/bin/env python3
"""This module is an entrypoint which creates required instances as threads and runs them."""
import logging
import signal
import sys
import time

from functools import partial

import prometheus_client

from aggregated_metrics_exporter import AggregatedMetricsExporter
from database_manager import DatabaseManager
from database_manager_token_price_collector import DatabaseManagerTokenPriceCollector
from scalar_metrics_exporter import ScalarMetricsExporter
from service_parameters import ServiceParameters
from utils import EXPECTED_NETWORK_EXCEPTIONS
from validators_info_exporter import ValidatorsInfoExporter


SECOND = 1

logger = logging.getLogger(__name__)


def main():
    """Create instances as threads and run them."""
    threads = []
    try:
        service_params = ServiceParameters()
        db_manager = DatabaseManager(service_params.database_url)
        if service_params.database_url_token_price_collector is None:
            db_manager_token_price = None
        else:
            db_manager_token_price = DatabaseManagerTokenPriceCollector(service_params.database_url_token_price_collector)
        threads.append(AggregatedMetricsExporter(database_manager=db_manager, service_params=service_params))
        threads.append(ScalarMetricsExporter(
            database_manager=db_manager,
            database_manager_token_price_collector=db_manager_token_price,
            service_params=service_params,
        ))
        threads.append(ValidatorsInfoExporter(database_manager=db_manager, service_params=service_params))
    except KeyboardInterrupt:
        sys.exit()
    except AssertionError as exc:
        sys.exit(f"The rule is violated: {type(exc)} - {exc}")
    except EXPECTED_NETWORK_EXCEPTIONS as exc:
        sys.exit(f"An expected exception occurred: {type(exc)} - {exc}")
    except Exception as exc:
        sys.exit(f"An unexpected exception occurred: {exc}")

    signal.signal(signal.SIGTERM, partial(
        stop_signal_handler,
        threads=threads,
        databases_managers=[db_manager, db_manager_token_price]
    ))
    signal.signal(signal.SIGINT, partial(
        stop_signal_handler,
        threads=threads,
        databases_managers=[db_manager, db_manager_token_price]
    ))

    for thread in threads:
        thread.start()

    # We don't start the server until aggregated and scalar metrics are initialized because each metric is initialized with a
    # default value (for Gauge that is 0) and there are fluctuations in Grafana. So we wait until metrics are set up and only
    # then we are able to start the Prometheus HTTP server.
    need_to_be_initialized = 2
    initialized = 0
    while need_to_be_initialized > initialized:
        logger.info("Waiting until metrics are initialized before starting the Prometheus client")
        time.sleep(SECOND)
        for thread in threads:
            if thread and hasattr(thread, 'initialized') and thread.initialized:
                initialized += 1
    prometheus_client.start_http_server(service_params.prometheus_metrics_port)

    for thread in threads:
        thread.join()


def stop_signal_handler(sig: int = None, frame=None, threads: list = None, databases_managers: [] = None):
    """Handle signal: close substrate interface connections and terminate the process"""
    logger.debug("Receiving signal: %s", sig)
    if threads:
        for thread in threads:
            if thread:
                with thread.lock:
                    thread.stop = True
                if thread.is_alive():
                    thread.join()

    if databases_managers:
        logger.info("Closing connections to databases managers")
        for database_manager in databases_managers:
            if database_manager:
                database_manager.conn.close()

    sys.exit(sig)


if __name__ == '__main__':
    main()
