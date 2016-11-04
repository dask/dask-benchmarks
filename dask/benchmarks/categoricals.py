# See https://github.com/pandas-dev/pandas/blob/master/asv_bench/benchmarks/categoricals.py
import pandas as pd
import numpy as np
from pandas import Series
import dask.dataframe as dd


class concat_categorical(object):
    goal_time = 0.2

    def setup(self):
        self.s = dd.from_pandas(
            pd.Series((list('aabbcd') * 1000000)).astype('category'),
            npartitions=10)

    def time_concat_categorical_interleave(self):
        dd.concat([self.s, self.s], interleave_partitions=True).compute()


class categorical_value_counts(object):
    goal_time = 1

    def setup(self):
        n = 500000
        np.random.seed(2718281)
        arr = ['s%04d' % i for i in np.random.randint(0, n // 10, size=n)]
        self.ts = dd.from_pandas(Series(arr).astype('category'),
                                 npartitions=10)

    def time_value_counts(self):
        self.ts.value_counts().compute()
