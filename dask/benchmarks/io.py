import os
import shutil
import string

import numpy as np
import pandas as pd
import dask
import dask.multiprocessing
import dask.dataframe as dd


def mkdf(rows=1000, files=50, n_floats=4, n_ints=4, n_strs=4):
    N = 1000
    floats = pd.DataFrame(np.random.randn(N, n_floats))
    ints = pd.DataFrame(np.random.randint(0, 100, size=(N, n_ints)),
                        columns=np.arange(n_floats, n_floats + n_ints))
    strs = pd.DataFrame(np.random.choice(list(string.ascii_letters),
                                         (N, n_strs)),
                        columns=np.arange(n_floats + n_ints,
                                          n_floats + n_ints + n_strs))

    df = pd.concat([floats, ints, strs], axis=1).rename(
        columns=lambda x: 'c_' + str(x))
    return df


class CSVSuite(object):
    params = [dask.get, dask.multiprocessing.get, dask.threaded.get]
    data_dir = 'benchmark_data'

    def setup_cache(self):
        df = mkdf()
        n_files = 50
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        for i in range(n_files):
            df.to_csv('{}/{}.csv'.format(self.data_dir, i), index=False)

    def teardown_cache(self):
        shutil.rmtree(self.data_dir)

    def time_read_csv_meta(self, get):
        return dd.read_csv('{}/*.csv'.format(self.data_dir))

    def time_read_csv(self, get):
        return dd.read_csv('{}/*.csv'.format(self.data_dir)).compute(get=get)


class HDF5Suite(object):
    params = [dask.get, dask.multiprocessing.get, dask.threaded.get]
    data_dir = 'benchmark_data'

    def setup_cache(self):
        df = mkdf()
        n_files = 50
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        for i in range(n_files):
            df.index += i * len(df)  # for unique index
            df.to_hdf('{}/{}.hdf5'.format(self.data_dir, i), 'key',
                      format='table')

    def setup(self, get):
        if get is dask.multiprocessing.get:
            raise NotImplementedError()

    def teardown_cache(self):
        shutil.rmtree(self.data_dir)

    def time_read_hdf5_meta(self, get):
        dd.read_hdf('{}/*.hdf5'.format(self.data_dir), 'key')

    def time_read_hdf5(self, get):
        (dd.read_hdf('{}/*.hdf5'.format(self.data_dir), 'key')
           .compute(get=get))
