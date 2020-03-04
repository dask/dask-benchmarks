import numpy as np
from dask import array as da

from .common import DaskSuite, rnd


class Rechunk(DaskSuite):

    SMALL_N = 20
    MEDIUM_N = 80

    def setup(self):
        small = rnd().random_sample((self.SMALL_N, self.SMALL_N))
        medium = rnd().random_sample((self.MEDIUM_N, self.MEDIUM_N))
        self.small = da.from_array(small, chunks=(small.shape[0], 1))
        self.medium = da.from_array(medium, chunks=(medium.shape[0], 1))

    def _rechunks(self, shape):
        m, n = shape
        assert m == n
        yield (1, n)
        yield (n, 1)
        yield (1, n)
        yield (n, 1)
        yield (1, n)

    def time_rechunk(self):
        z = self.small
        for r in self._rechunks(z.shape):
            z = z.rechunk(r)
        z.compute()

    def time_rechunk_meta(self):
        z = self.medium
        for r in self._rechunks(z.shape):
            z = z.rechunk(r)


class FancyIndexing(DaskSuite):

    def setup(self):
        r = rnd()
        self.a = da.empty(shape=(2000000, 200, 2), dtype='i1',
                          chunks=(10000, 100, 2))
        self.c = r.randint(0, 2, size=self.a.shape[0], dtype=bool)
        self.s = sorted(r.choice(self.a.shape[1], size=100, replace=False))

    def time_fancy(self):
        self.a[self.c][:, self.s]


class Slicing(DaskSuite):

    def setup(self):
        self.N = 100000
        self.a = da.empty(shape=(self.N,), dtype='i1',
                          chunks=[1] * self.N)

    def time_slice_slice_head(self):
        self.a[slice(10, 51, None)].compute()

    def time_slice_slice_tail(self):
        self.a[slice(self.N - 51, self.N - 10, None)].compute()

    def time_slice_int_tail(self):
        self.a[self.N - 51].compute()

    def time_slice_int_head(self):
        self.a[51].compute()

    def time_slices_from_chunks(self):
        da.core.slices_from_chunks(((2,) * 1000, (3,) * 1000, (4,) * 10))


class TestSubs(DaskSuite):
    def setup(self):
        x = da.ones((50, 50), chunks=(10, 10))
        self.arr = (x.reshape(-1, 1).rechunk(50, 2)
                     .reshape(50, 50).rechunk(25, 25)
                     .reshape(1, -1) .reshape(50, 50))

    def time_subs(self):
        self.arr.compute()


class Blockwise(DaskSuite):
    def setup(self):
        A = 400
        B = 800

        a = da.ones((A, B, 2), chunks=1)
        b = da.zeros((A, B, 1), chunks=1)
        c = a + b
        g = c.__dask_graph__()
        layer = g.layers[c.name]
        self.layer = layer

    def time_make_blockwise_graph(self):
        self.layer._dict


def combine(x, y, block_id):
    return x + y


class BlockInfoBlockwise(DaskSuite):
    def setup(self):
        CHUNK_SIZE = 10
        NCHUNKS = 9000
        SIZE = CHUNK_SIZE * NCHUNKS

        base = [da.full((SIZE,), i, dtype=np.int8, chunks=CHUNK_SIZE) for i in range(4)]
        self.base = base

    def time_optimize(self):
        base = self.base

        a = base[0] + base[1]
        b = da.map_blocks(combine, a, base[2], dtype=np.int8)
        c = b + base[3]
        return c

    def time_compute(self):
        c = self.time_optimize()
        c.compute()
