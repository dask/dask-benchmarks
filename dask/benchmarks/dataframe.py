import string

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


class TimeFloatConstructors(object):

    def setup(self):
        self.floats = np.random.randn(10000, 10)
        self.wide_floats = np.random.randn(10000, 1000)
        self.floats_pandas = pd.DataFrame(self.floats)
        self.wide_floats_pandas = pd.DataFrame(self.wide_floats)

    def time_floats(self):
        dd.from_array(self.floats)

    def time_wide_floats(self):
        dd.from_array(self.wide_floats)

    def time_floats_pandas(self):
        dd.from_pandas(self.floats_pandas, npartitions=2)

    def time_wide_floats_pandas(self):
        dd.from_pandas(self.wide_floats_pandas, npartitions=2)


class TimeIntConstructors(object):

    def setup(self):
        self.ints = np.random.randint(0, 100, size=(10000, 10))
        self.wide_ints = np.random.randint(0, 100, size=(10000, 1000))
        self.ints_pandas = pd.DataFrame(self.ints)
        self.wide_ints_pandas = pd.DataFrame(self.wide_ints)

    def time_ints(self):
        dd.from_array(self.ints)

    def time_wide_ints(self):
        dd.from_array(self.wide_ints)

    def time_ints_pandas(self):
        dd.from_pandas(self.ints_pandas, npartitions=2)

    def time_wide_ints_pandas(self):
        dd.from_pandas(self.wide_ints_pandas, npartitions=2)


class TimeObjectConstructors(object):

    def setup(self):
        self.text = np.random.choice(list(string.ascii_letters),
                                     size=(10000, 10))
        self.wide_text = np.random.choice(list(string.ascii_letters),
                                          size=(10000, 1000))
        self.text_pandas = pd.DataFrame(self.text)
        self.wide_text_pandas = pd.DataFrame(self.wide_text)

    def time_text(self):
        dd.from_array(self.text)

    def time_wide_text(self):
        dd.from_array(self.wide_text)

    def time_text_pandas(self):
        dd.from_pandas(self.text_pandas, npartitions=2)

    def time_wide_text_pandas(self):
        dd.from_pandas(self.wide_text_pandas, npartitions=2)
