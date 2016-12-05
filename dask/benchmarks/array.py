from dask import array as da
import numpy as np

from .common import DaskSuite, rnd


class Rechunk(DaskSuite):

    N = 20

    def setup(self):
        a = rnd().random_sample((self.N, self.N))
        self.z = da.from_array(a, chunks=(self.N, 1))

    def time_rechunk(self):
        z = self.z
        z = z.rechunk((1, self.N))
        z = z.rechunk((self.N, 1))
        z = z.rechunk((1, self.N))
        z = z.rechunk((self.N, 1))
        z = z.rechunk((1, self.N))
        z.compute()


class FancyIndexing(DaskSuite):

    def setup(self):
        r = rnd()
        self.a = da.empty(shape=(2000000, 200, 2), dtype='i1',
                          chunks=(10000, 100, 2))
        self.c = r.randint(0, 2, size=self.a.shape[0], dtype=bool)
        self.s = sorted(r.choice(self.a.shape[1], size=100, replace=False))

    def time_fancy(self):
        self.a[self.c][:, self.s]
