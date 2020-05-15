import numpy as np
from pandas import DataFrame
import pandas as pd
import dask.dataframe as dd


class nogil_groupby_base(object):
    goal_time = 0.2

    def setup(self):
        self.N = 1000000
        self.ngroups = 1000
        np.random.seed(1234)
        self.df = DataFrame({'key': np.random.randint(0, self.ngroups,
                                                      size=self.N),
                             'data': np.random.randn(self.N)})
        self.df = dd.from_pandas(self.df, npartitions=4)

    def time_nogil_groupby_count_2(self):
        self.df.groupby('key')['data'].count().compute()

    # def time_nogil_groupby_last_2(self):
    #     self.df.groupby('key')['data'].last().compute()

    def time_nogil_groupby_max_2(self):
        self.df.groupby('key')['data'].max().compute()

    def time_nogil_groupby_mean_2(self):
        self.df.groupby('key')['data'].mean().compute()

    def time_nogil_groupby_min_2(self):
        self.df.groupby('key')['data'].min().compute()

    # def time_nogil_groupby_prod_2(self):
    #     self.df.groupby('key')['data'].prod().compute()

    def time_nogil_groupby_sum_2(self):
        self.df.groupby('key')['data'].sum().compute()

    def time_nogil_groupby_sum_4(self):
        self.df.groupby('key')['data'].sum().compute()

    def time_nogil_groupby_sum_8(self):
        self.df.groupby('key')['data'].sum()

    def time_nogil_groupby_var_2(self):
        self.df.groupby('key')['data'].var()


class nogil_n_larget(object):
    def setup(self):
        np.random.seed(1234)
        self.N = 10000000
        self.k = 500000
        self.a = np.random.randn(self.N)
        self.s = dd.from_array(self.a)

    def time_nogil_n_largest(self):
        self.s.nlargest(n=5).compute()


class nogil_datetime_fields(object):
    goal_time = 0.2

    def setup(self):
        self.N = 1000000
        self.dti = pd.date_range('1900-01-01', periods=self.N, freq='D')
        # TODO: dt namespace on Periods
        # self.period = self.dti.to_period('D')

        self.dti = dd.from_pandas(pd.Series(self.dti), npartitions=4)
        # self.period = dd.from_pandas(pd.Series(self.period), npartitions=4)

    def time_datetime_field_year(self):
        self.dti.dt.year.compute()

    def time_datetime_field_day(self):
        self.dti.dt.day.compute()

    def time_datetime_field_daysinmonth(self):
        self.dti.dt.days_in_month.compute()

    def time_datetime_field_normalize(self):
        self.dti.dt.normalize().compute()

    def time_datetime_to_period(self):
        self.dti.dt.to_period('S').compute()

    # def time_period_to_datetime(self):
    #     def run(period):
    #         period.to_timestamp()
    #     run(self.period)


class nogil_rolling_algos_slow(object):
    goal_time = 0.2

    def setup(self):
        self.win = 100
        np.random.seed(1234)
        self.arr = np.random.rand(100000)
        self.arr = dd.from_array(self.arr)

    def time_nogil_rolling_median(self):
        self.arr.rolling(self.win).median().compute()

    def time_nogil_rolling_mean(self):
        self.arr.rolling(self.win).mean().compute()

    def time_nogil_rolling_min(self):
        self.arr.rolling(self.win).min().compute()

    def time_nogil_rolling_max(self):
        self.arr.rolling(self.win).max().compute()

    def time_nogil_rolling_var(self):
        self.arr.rolling(self.win).var().compute()

    def time_nogil_rolling_skew(self):
        self.arr.rolling(self.win).skew().compute()

    def time_nogil_rolling_kurt(self):
        self.arr.rolling(self.win).kurt().compute()

    def time_nogil_rolling_std(self):
        self.arr.rolling(self.win).std().compute()
