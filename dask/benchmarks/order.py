from dask import array as da
from dask.base import collections_to_dsk
from dask.order import order
from .common import DaskSuite


def f(*args):
    pass


def fully_connected_layers(width, height):
    """ Create a (very artificial) DAG of given dimensions.

    Each layer is fully connected to the previous layer.
    """
    dsk = {(0, w): (f,) for w in range(width)}
    for h in range(1, height):
        task = (f,) + tuple((h - 1, w) for w in range(width))
        for w in range(width):
            dsk[(h, w)] = task
    return dsk


def create_disconnected_subgraphs(num_groups, width, height):
    dsk = {}
    for index in range(num_groups):
        dsk.update({(index, 0, w): (f,) for w in range(width)})
        for h in range(1, height):
            for w in range(width):
                dsk[(index, h, w)] = (
                    f,
                    (index, h - 1, w),
                    (index, h - 1, (w + 1) % width),
                )
    return dsk


class OrderMapOverlap(DaskSuite):
    params = [[
        ((1e4, 1e4), (200, 200), 1),
        ((1000, 1000, 1000), (100, 100, 100), 10),
    ]]

    def setup(self, param):
        size, chunks, depth = param
        a = da.random.random(size, chunks=chunks)
        b = a.map_overlap(lambda e: 2 * e, depth=depth)
        self.dsk = collections_to_dsk([b])

    def time_order_mapoverlap(self, param):
        order(self.dsk)


class OrderSVD(DaskSuite):
    def setup(self):
        a = da.random.random((6000, 64), chunks=(10, 64))
        u, s, v = da.linalg.svd_compressed(a, 100, iterator="power", n_power_iter=0)
        self.dsk_svd = collections_to_dsk([u, s, v])

    def time_order_svd(self):
        order(self.dsk_svd)


class OrderRechunkTranspose(DaskSuite):
    def setup(self):
        a = da.random.normal(size=(4e6, 30e2), chunks=(2e4, 3e1))
        a = a.rechunk((int(1e4 / 10), int(30e2)))
        b = a.T.dot(a)
        self.dsk_rechunk_transpose = collections_to_dsk([b])

    def time_order_rechunk_transpose(self):
        order(self.dsk_rechunk_transpose)


class OrderLinalgSolves(DaskSuite):
    def setup(self):
        n = 1000
        x = da.random.normal(size=(n, 100), chunks=(1, 100))
        y = da.random.normal(size=(n,), chunks=(1,))
        xy = (x * y[:, None]).cumsum(axis=0)
        xx = (x[:, None, :] * x[:, :, None]).cumsum(axis=0)
        beta = da.stack(
            [da.linalg.solve(xx[i], xy[i]) for i in range(xx.shape[0])], axis=0
        )
        ey = (x * beta).sum(axis=1)
        self.dsk_linalg = collections_to_dsk([ey])

    def time_order_linalg_solve(self):
        order(self.dsk_linalg)


class OrderFullLayers(DaskSuite):
    params = [[
        (1, 50000),
        (2, 10000),
        (10, 1000),
        (100, 20),
        (500, 2),
        (9999, 1),
        (50000, 1),
    ]]

    def setup(self, param):
        width, height = param
        self.dsk = fully_connected_layers(width, height)

    def time_order_full_layers(self, param):
        order(self.dsk)


class OrderLinearWithDanglers(DaskSuite):
    params = [[
        (2, 10000),
        (5, 5000),
    ]]

    def setup(self, param):
        width, height = param
        self.dsk = {(0, 0): (f,)}
        for i in range(1, height):
            for j in range(width):
                self.dsk[(i, j)] = (f, (i - 1, 0))

    def time_order_linear_danglers(self, param):
        order(self.dsk)


class OrderLinearFull(DaskSuite):
    def setup(self):
        # Although not a realistic DAG, this is cleverly constructed to stress
        # a current weakness of `order` in https://github.com/dask/dask/pull/5646
        # Specifically, nodes use a tie-breaker `num_dependents - height` in
        # order to prefer "tall and narrow".  Here, all non-root nodes have the
        # same height, so we prefer "narrow"--i.e., fewer dependents--which means
        # we choose the node that is least ready to compute.  So, this is a worst
        # case scenario.  This is unlikely to occur in practice, because task
        # fusion should fuse this into a single task.
        self.dsk = {0: (f,)}
        prev = (f, 0)
        for i in range(1, 1000):
            self.dsk[i] = prev
            prev += (i,)

    def time_order_linear_full(self):
        order(self.dsk)


class OrderManySubgraphs(DaskSuite):
    """This tests behavior when there are few or many disconnected subgraphs"""
    params = [[
        (1, 9999),
        (3, 3333),
        (10, 999),
        (30, 303),
        (100, 99),
        (999, 10),
    ]]

    def setup(self, param):
        num_subgraphs, width = param
        self.dsk = create_disconnected_subgraphs(num_subgraphs, width, 2)

    def time_order_many_subgraphs(self, param):
        order(self.dsk)


class OrderCholesky(DaskSuite):
    def setup(self):
        n = 50
        A = da.random.random((n, n), chunks=(1, 1))
        self.dsk = collections_to_dsk([da.linalg.cholesky(A)])
        self.dsk_lower = collections_to_dsk([da.linalg.cholesky(A, lower=True)])

    def time_order_cholesky(self):
        order(self.dsk)

    def time_order_cholesky_lower(self):
        order(self.dsk_lower)


class OrderCholeskyMixed(DaskSuite):
    def setup(self):
        n = 16
        A = da.random.random((n, n), chunks=(1, 1))
        Bs = [A]
        # The top-left of A is shared by all the Bs.  For example, for i=2:
        # AAB.B
        # AAB.B
        # BBB.B
        # ....B
        # BBBBB
        for i in range(1, n):
            B = da.random.random((i, i), chunks=(1, 1))
            B = da.concatenate([da.concatenate([B, A.blocks[i:, :i]]), A.blocks[:, i:]], axis=1)
            Bs.append(B)
        self.dsk = collections_to_dsk([da.linalg.cholesky(B) for B in Bs])
        self.dsk_lower = collections_to_dsk([da.linalg.cholesky(B, lower=True) for B in Bs])

    def time_order_cholesky_mixed(self):
        order(self.dsk)

    def time_order_cholesky_mixed_lower(self):
        order(self.dsk_lower)
