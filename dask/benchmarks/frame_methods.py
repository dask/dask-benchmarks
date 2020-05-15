import string

import numpy as np
from numpy.random import randn
from pandas import DataFrame, Series, date_range, NaT
import dask.dataframe as dd


class frame_apply_axis_1(object):
    goal_time = 0.2

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(100000, 100)), 2)

    def time_frame_apply_axis_1(self):
        self.df.apply((lambda x: (x + 1)), axis=1)

    def time_frame_apply_lambda_mean(self):
        self.df.apply((lambda x: x.sum()), axis=1)

    def time_frame_apply_np_mean(self):
        self.df.apply(np.mean, axis=1)

    def time_frame_apply_pass_thru(self):
        self.df.apply((lambda x: x), axis=1)


class frame_apply_ref_by_name(object):
    goal_time = 0.2

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(100000, 3),
                                           columns=list('ABC')),
                                 npartitions=2)

    def time_frame_apply_ref_by_name(self):
        self.df.apply((lambda x: (x['A'] + x['B'])), axis=1)


class frame_apply_user_func(object):
    goal_time = 0.2

    def setup(self):
        self.s = Series(np.arange(1028.0))
        self.df = dd.from_pandas(DataFrame({i: self.s for i in range(1028)}),
                                 npartitions=2)

    def time_frame_apply_user_func(self):
        self.df.apply((lambda x: np.corrcoef(x, self.s)[(0, 1)]), axis=1)


# class frame_boolean_row_select(object):
#     goal_time = 0.2

#     def setup(self):
#         self.df = dd.from_pandas(DataFrame(randn(10000, 100)),
#                                  npartitions=2)
#         self.bool_arr = np.zeros(10000, dtype=bool)
#         self.bool_arr[:100] = True

#     def time_frame_boolean_row_select(self):
#         self.df.loc[self.bool_arr, :]


class frame_dropna(object):
    goal_time = 0.2

    def setup(self):
        self.data = np.random.randn(10000, 1000)
        self.df = DataFrame(self.data)
        self.df.loc[50:1000, 20:50] = np.nan
        self.df.loc[2000:3000] = np.nan
        self.df.loc[:, 60:70] = np.nan
        self.df = dd.from_pandas(self.df, npartitions=2)

    def time_frame_dropna_axis0_all(self):
        self.df.dropna(how='all')

    def time_frame_dropna_axis0_any(self):
        self.df.dropna(how='any')



class frame_dropna_axis0_mixed_dtypes(object):
    goal_time = 0.2

    def setup(self):
        self.data = np.random.randn(10000, 1000)
        self.df = DataFrame(self.data)
        self.df.loc[50:1000, 20:50] = np.nan
        self.df.loc[2000:3000] = np.nan
        self.df.loc[:, 60:70] = np.nan
        self.df['foo'] = 'bar'
        self.df = dd.from_pandas(self.df, npartitions=2)

    def time_frame_dropna_axis0_all_mixed_dtypes(self):
        self.df.dropna(how='all')

    def time_frame_dropna_axis0_any_mixed_dtypes(self):
        self.df.dropna(how='any')



class frame_dtypes(object):
    goal_time = 0.2

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(1000, 1000)),
                                 npartitions=2)

    def time_frame_dtypes(self):
        self.df.dtypes


class frame_duplicated(object):
    goal_time = 0.2

    def setup(self):
        self.n = (1 << 20)
        self.t = date_range('2015-01-01', freq='S', periods=(self.n // 64))
        self.xs = np.random.randn((self.n // 64)).round(2)
        self.df = dd.from_pandas(
            DataFrame({'a': np.random.randint(((-1) << 8), (1 << 8), self.n),
                       'b': np.random.choice(self.t, self.n),
                       'c': np.random.choice(self.xs, self.n), }),
            npartitions=2)

    def time_frame_duplicated(self):
        self.df.drop_duplicates()


class frame_float_equal(object):
    goal_time = 0.2

    def setup(self):
        self.float_df = DataFrame(np.random.randn(1000, 1000))
        self.object_df = DataFrame(([(['foo'] * 1000)] * 1000))
        self.nonunique_cols = self.object_df.copy()
        self.nonunique_cols.columns = (['A'] * len(self.nonunique_cols.columns))
        self.pairs = dict([(name, self.make_pair(frame)) for (name, frame) in (('float_df', self.float_df), ('object_df', self.object_df), ('nonunique_cols', self.nonunique_cols))])

    def time_frame_float_equal(self):
        self.test_equal('float_df')

    def make_pair(self, frame):
        self.df = frame
        self.df2 = self.df.copy()
        self.df2.ix[((-1), (-1))] = np.nan
        return (self.df, self.df2)

    def test_equal(self, name):
        (self.df, self.df2) = self.pairs[name]
        return self.df.equals(self.df)

    def test_unequal(self, name):
        (self.df, self.df2) = self.pairs[name]
        return self.df.equals(self.df2)


class frame_float_unequal(object):
    goal_time = 0.2

    def setup(self):
        self.float_df = DataFrame(np.random.randn(1000, 1000))
        self.object_df = DataFrame(([(['foo'] * 1000)] * 1000))
        self.nonunique_cols = self.object_df.copy()
        self.nonunique_cols.columns = (['A'] * len(self.nonunique_cols.columns))
        self.pairs = dict([(name, self.make_pair(frame)) for (name, frame) in (('float_df', self.float_df), ('object_df', self.object_df), ('nonunique_cols', self.nonunique_cols))])

    def time_frame_float_unequal(self):
        self.test_unequal('float_df')

    def make_pair(self, frame):
        self.df = frame
        self.df2 = self.df.copy()
        self.df2.ix[((-1), (-1))] = np.nan
        return (self.df, self.df2)

    def test_equal(self, name):
        (self.df, self.df2) = self.pairs[name]
        return self.df.equals(self.df)

    def test_unequal(self, name):
        (self.df, self.df2) = self.pairs[name]
        return self.df.equals(self.df2)


class frame_isnull_floats_no_null(object):
    goal_time = 0.2

    def setup(self):
        self.data = np.random.randn(100000, 1000)
        self.df = dd.from_pandas(DataFrame(self.data), npartitions=4)

    def time_frame_isnull(self):
        self.df.isnull()


class frame_isnull_floats(object):
    goal_time = 0.2

    def setup(self):
        np.random.seed(1234)
        self.sample = np.array([np.nan, 1.0])
        self.data = np.random.choice(self.sample, (100000, 1000))
        self.df = dd.from_pandas(DataFrame(self.data), npartitions=4)

    def time_frame_isnull(self):
        self.df.isnull()


class frame_isnull_strings(object):
    goal_time = 0.2

    def setup(self):
        np.random.seed(1234)
        self.sample = np.array(list(string.ascii_lowercase) +
                               list(string.ascii_uppercase) +
                               list(string.whitespace))
        self.data = np.random.choice(self.sample, (100000, 1000))
        self.df = dd.from_pandas(DataFrame(self.data), npartitions=4)

    def time_frame_isnull(self):
        self.df.isnull()


class frame_isnull_obj(object):
    goal_time = 0.2

    def setup(self):
        np.random.seed(1234)
        self.sample = np.array([NaT, np.nan, None, np.datetime64('NaT'),
                                np.timedelta64('NaT'), 0, 1, 2.0, '', 'abcd'])
        self.data = np.random.choice(self.sample, (10000, 100))
        self.df = dd.from_pandas(DataFrame(self.data), npartitions=4)

    def time_frame_isnull(self):
        self.df.isnull()


class frame_itertuples(object):

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(50000, 10)),
                                 npartitions=4)

    def time_frame_itertuples(self):
        for row in self.df.itertuples():
            pass


class series_string_vector_slice(object):
    goal_time = 0.2

    def setup(self):
        self.s = dd.from_pandas(Series((['abcdefg', np.nan] * 500000)),
                                npartitions=4)

    def time_series_string_vector_slice(self):
        self.s.str.slice(5)


class frame_quantile_axis1(object):
    goal_time = 0.2

    def setup(self):
        self.df = dd.from_pandas(DataFrame(np.random.randn(100000, 3),
                                           columns=list('ABC')),
                                 npartitions=4)

    def time_frame_quantile_axis1(self):
        self.df.quantile(0.1, axis=1)
