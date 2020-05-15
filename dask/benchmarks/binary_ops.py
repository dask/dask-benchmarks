from pandas import DataFrame, Series, date_range
import numpy as np
import pandas.computation.expressions as expr
import dask.dataframe as dd


class frame_add(object):
    goal_time = 0.2

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(20000, 100)),
                                 npartitions=10)
        self.df2 = dd.from_pandas(DataFrame(np.random.randn(20000, 100)),
                                  npartitions=10)

    def time_frame_add(self):
        (self.df + self.df2)


class frame_add_no_ne(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_use_numexpr(False)

    def time_frame_add_no_ne(self):
        (self.df + self.df2)

    def teardown(self):
        expr.set_use_numexpr(True)


class frame_add_st(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_numexpr_threads(1)

    def time_frame_add_st(self):
        (self.df + self.df2)

    def teardown(self):
        expr.set_numexpr_threads()


class frame_float_div(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(1000, 1000))
        self.df2 = DataFrame(np.random.randn(1000, 1000))

    def time_frame_float_div(self):
        (self.df // self.df2)


class frame_float_div_by_zero(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(1000, 1000))

    def time_frame_float_div_by_zero(self):
        (self.df / 0)


class frame_float_floor_by_zero(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(1000, 1000))

    def time_frame_float_floor_by_zero(self):
        (self.df // 0)


class frame_float_mod(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(1000, 1000))
        self.df2 = DataFrame(np.random.randn(1000, 1000))

    def time_frame_float_mod(self):
        (self.df / self.df2)


class frame_int_div_by_zero(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.random_integers(np.iinfo(np.int16).min, np.iinfo(np.int16).max, size=(1000, 1000)))

    def time_frame_int_div_by_zero(self):
        (self.df / 0)


class frame_int_mod(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.random_integers(np.iinfo(np.int16).min, np.iinfo(np.int16).max, size=(1000, 1000)))
        self.df2 = DataFrame(np.random.random_integers(np.iinfo(np.int16).min, np.iinfo(np.int16).max, size=(1000, 1000)))

    def time_frame_int_mod(self):
        (self.df / self.df2)


class frame_mult(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))

    def time_frame_mult(self):
        (self.df * self.df2)


class frame_mult_no_ne(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_use_numexpr(False)

    def time_frame_mult_no_ne(self):
        (self.df * self.df2)

    def teardown(self):
        expr.set_use_numexpr(True)


class frame_mult_st(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_numexpr_threads(1)

    def time_frame_mult_st(self):
        (self.df * self.df2)

    def teardown(self):
        expr.set_numexpr_threads()


class frame_multi_and(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))

    def time_frame_multi_and(self):
        self.df[((self.df > 0) & (self.df2 > 0))]


class frame_multi_and_no_ne(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_use_numexpr(False)

    def time_frame_multi_and_no_ne(self):
        self.df[((self.df > 0) & (self.df2 > 0))]

    def teardown(self):
        expr.set_use_numexpr(True)


class frame_multi_and_st(object):
    goal_time = 0.2

    def setup(self):
        self.df = DataFrame(np.random.randn(20000, 100))
        self.df2 = DataFrame(np.random.randn(20000, 100))
        expr.set_numexpr_threads(1)

    def time_frame_multi_and_st(self):
        self.df[((self.df > 0) & (self.df2 > 0))]

    def teardown(self):
        expr.set_numexpr_threads()


class series_timestamp_compare(object):
    goal_time = 0.2

    def setup(self):
        self.N = 1000000
        self.halfway = ((self.N // 2) - 1)
        self.s = Series(date_range('20010101', periods=self.N, freq='T'))
        self.ts = self.s[self.halfway]

    def time_series_timestamp_compare(self):
        (self.s <= self.ts)


class timestamp_ops_diff1(object):
    goal_time = 0.2
    N = 1000000

    def setup(self):
        self.s = self.create()

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='s'))

    def time_timestamp_ops_diff1(self):
        self.s.diff()

class timestamp_tz_ops_diff1(timestamp_ops_diff1):
    N = 10000

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='s', tz='US/Eastern'))

class timestamp_ops_diff2(object):
    goal_time = 0.2
    N = 1000000

    def setup(self):
        self.s = self.create()

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='s'))

    def time_timestamp_ops_diff2(self):
        (self.s - self.s.shift())

class timestamp_tz_ops_diff2(timestamp_ops_diff2):
    N = 10000

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='s', tz='US/Eastern'))

class timestamp_series_compare(object):
    goal_time = 0.2
    N = 1000000

    def setup(self):
        self.halfway = ((self.N // 2) - 1)
        self.s = self.create()
        self.ts = self.s[self.halfway]

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='T'))

    def time_timestamp_series_compare(self):
        (self.ts >= self.s)

class timestamp_tz_series_compare(timestamp_series_compare):
    N = 10000

    def create(self):
        return Series(date_range('20010101', periods=self.N, freq='T', tz='US/Eastern'))
