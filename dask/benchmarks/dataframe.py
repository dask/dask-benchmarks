import numpy as np
import pandas as pd

import dask
import dask.dataframe as dd

from benchmarks.common import DaskSuite, rnd


class MemoryDataFrame(DaskSuite):
    N = 10000000

    def setup(self):
        r = rnd()
        df = pd.DataFrame(r.randn(self.N, 2), columns=list("ab"))
        df["c"] = r.randint(0, 500, size=(self.N,))
        self.data = dd.from_pandas(df, npartitions=5)
        self.smaller_data = dd.from_pandas(df[: self.N // 10], npartitions=5)

    def time_set_index(self):
        self.smaller_data.set_index("a").compute()

    def time_count_values(self):
        self.data.c.value_counts().compute()

    def time_groupby(self):
        self.data.groupby("c").agg(["mean", "max", "min"]).compute()

    def time_scalar_comparison(self):
        (self.data.a > 0).compute()

    def time_reduction(self):
        self.data.reduction(np.max).compute()

    def time_boolean_indexing(self):
        self.data[(self.data.a > 0) & (self.data.b > 0)].compute()

    def test_random_split(self):
        dask.compute(self.data.random_split([0.25, 0.75]))

    def test_repartition(self):
        self.data.repartition(npartitions=2).compute()

    def test_quantile(self):
        self.data.quantile(0.25).compute()
