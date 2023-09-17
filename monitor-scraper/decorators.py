"""This module contains decorators that are used to remove the duplicated code and count exceptions to alert if needed."""
import functools

from datetime import datetime

from prometheus_metrics import metrics_exporter


TIME_THRESHOLD_SECS = 180


class ReconnectionCounter:
    """This class contains the method repeater_counter which is used as a decorator for the main function in threads."""
    prev_exception_occurred: dict

    def __init__(self):
        self.prev_exception_occurred = {}

    def reconnection_counter(self, func):
        """This method is used as a decorator to count exceptions and alert if needed."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            thread_name = func.__doc__
            func(*args, **kwargs)
            if thread_name not in self.prev_exception_occurred:
                self.prev_exception_occurred[thread_name] = datetime.now()
            else:
                if (self.prev_exception_occurred[thread_name] - datetime.now()).total_seconds() < TIME_THRESHOLD_SECS:
                    metrics_exporter.alert_thread_is_failed.labels(thread_name).set(int(True))
                else:
                    self.prev_exception_occurred[thread_name] = datetime.now()
            return func(*args, **kwargs)

        return wrapper

    def remove_thread(self, thread_name: str):
        """Remove the thread from the dict"""
        if thread_name in self.prev_exception_occurred:
            self.prev_exception_occurred.pop(thread_name)


reconnection_counter = ReconnectionCounter()
