import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd


class MemoryDataFrameSuite(object):

    goal_time = 0.2

    def setup(self):
        N = 10000000
        df = pd.DataFrame(np.random.randn(N, 2), columns=list('ab'))
        df['c'] = np.random.randint(0, 500, size=(N,))
        a = dd.from_pandas(df, npartitions=5)
        self.data = a

    def time_set_index(self):
        self.data.set_index('a').compute()

    def time_count_values(self):
        self.data.c.value_counts().compute()

    def time_groupby(self):
        self.data.groupby('c').agg(['mean', 'max', 'min']).compute()

    def time_scalar_comparison(self):
        (self.data.a > 0).compute()

    def time_reduction(self):
        self.data.reduction(np.max).compute()

    def time_boolean_indexing(self):
        self.data[(self.data.a > 0) & (self.data.b > 0)].compute()

    def test_random_split(self):
        dask.compute(self.data.random_split([.25, .75]))

    def test_repartition(self):
        self.data.repartition(npartitions=2).compute()

    def test_quantile(self):
        self.data.quantile(.25).compute()
