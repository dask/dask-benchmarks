
from dask.optimization import fuse_linear
from dask import array as da
import numpy as np
from .common import DaskSuite


class Block3D(DaskSuite):
    """This creates a (5n)^3 block matrix.

    This is very similar to the numpy benchmark Block3D.

    In this benchmark, we are comparing the performance of dask.array.block
    to that of numpy and a direct memory copy of the array.

    We also compare the optimized version and unoptimized version of the
    computation as well as the performance of concatenating 1D versions of
    the dask arrays.

    Finally, we also ensure that a call to persist on a 3D block doesn't
    copy memory around by returning in a minimal amount of time.
    """
    #
    # Having all these modes puts the plots on the same graph
    # as opposed to being displayed as separate benchmarks
    params = [[1, 10, 100],
              ['block', 'block optimized',
               'block persist', 'block optimized persist',
               'concatenate',
               'np_block', 'np_copy']]
    param_names = ['n', 'mode']

    def setup(self, n, mode):
        dtype = 'uint64'
        self.n000 = np.full((2 * n, 2 * n, 2 * n), fill_value=1, dtype=dtype)
        self.n001 = np.full((2 * n, 2 * n, 3 * n), fill_value=4, dtype=dtype)

        self.n010 = np.full((2 * n, 3 * n, 2 * n), fill_value=3, dtype=dtype)
        self.n011 = np.full((2 * n, 3 * n, 3 * n), fill_value=5, dtype=dtype)

        self.n100 = np.full((3 * n, 2 * n, 2 * n), fill_value=2, dtype=dtype)
        self.n101 = np.full((3 * n, 2 * n, 3 * n), fill_value=6, dtype=dtype)

        self.n110 = np.full((3 * n, 3 * n, 2 * n), fill_value=7, dtype=dtype)
        self.n111 = np.full((3 * n, 3 * n, 3 * n), fill_value=8, dtype=dtype)

        self.d000 = da.from_array(self.n000, chunks=-1).persist()
        self.d001 = da.from_array(self.n001, chunks=-1).persist()
        self.d010 = da.from_array(self.n010, chunks=-1).persist()
        self.d011 = da.from_array(self.n011, chunks=-1).persist()
        self.d100 = da.from_array(self.n100, chunks=-1).persist()
        self.d101 = da.from_array(self.n101, chunks=-1).persist()
        self.d110 = da.from_array(self.n110, chunks=-1).persist()
        self.d111 = da.from_array(self.n111, chunks=-1).persist()

        self.np_block = [
            [
                [self.n000, self.n001],
                [self.n010, self.n011],
            ],
            [
                [self.n100, self.n101],
                [self.n110, self.n111],
            ]
        ]
        self.np_arr_list = [a.flat
                            for two_d in self.np_block
                            for one_d in two_d
                            for a in one_d]

        self.block = [
            [
                [self.d000, self.d001],
                [self.d010, self.d011],
            ],
            [
                [self.d100, self.d101],
                [self.d110, self.d111],
            ]
        ]
        self.arr_list = [da.ravel(d)
                         for two_d in self.block
                         for one_d in two_d
                         for d in one_d]

        self.da_block = da.block(self.block)
        self.da_concatenate = da.concatenate(self.arr_list)
        if mode.startswith('block optimized'):
            self.da_block.dask, _ = fuse_linear(self.da_block.dask)


    def time_3d(self, n, mode):
        if mode.startswith('block'):
            if mode.endswith('persist'):
                self.da_block.persist()
            else:
                self.da_block.compute()
        elif mode.startswith('concatenate'):
            if mode.endswith('persist'):
                self.da_concatenate.persist()
            else:
                self.da_concatenate.compute()
        elif mode == 'np_block':
            np.block(self.np_block)
        else:
            [arr.copy() for arr in self.np_arr_list]
