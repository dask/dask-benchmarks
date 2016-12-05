
import time
import timeit

import numpy as np


def rnd():
    return np.random.RandomState(42)


class DaskSuite(object):

    goal_time = 2.0

    # See https://asv.readthedocs.io/en/latest/writing_benchmarks.html#timing
    timer = timeit.default_timer

    # XXX: add scheduler-related initialization for more stable benchmarks?
    # (nthreads, CPU affinity, etc.?)
