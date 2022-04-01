from timeit import default_timer as timer
from queue import LifoQueue

_performance_trackers = LifoQueue()


def start_performance_tracking():
    """Start a performance timer"""
    tracker = timer()
    _performance_trackers.put(tracker)
    return tracker


def end_performance_tracking() -> float:
    """End a performance timer and return the difference in seconds"""
    tracker = _performance_trackers.get()
    difference = timer() - tracker
    return difference


def track_runtime(func, **kwargs):
    """
    Wrap a function with performance trackers, return a tuple representing the function return value, and the time
    in seconds to perform said wrapped function
    """
    start = timer()
    ret = func(**kwargs)
    return ret, timer() - start
