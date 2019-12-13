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
    def setup(self):
        a = da.random.random((1000, 1000), chunks=(100, 100))
        b = a.map_overlap(lambda e: 2 * e, depth=1)
        self.dsk_medium = collections_to_dsk(b)

        a = da.random.random((1e4, 1e4), chunks=(512, 512))
        b = a.map_overlap(lambda e: 2 * e, depth=10)
        self.dsk_large = collections_to_dsk(b)

    def time_order_mapoverlap_medium(self):
        order(self.dsk_medium)

    def time_order_mapoverlap_large(self):
        order(self.dsk_large)


class OrderSVD(DaskSuite):
    def setup(self):
        a = da.random.random((6000, 64), chunks=(10, 64))
        u, s, v = da.linalg.svd_compressed(a, 100, 0)
        self.dsk_svd = collections_to_dsk([u, s, v])

    def time_order_svd(self):
        order(self.dsk_svd)


class OrderRechunkTranspose(DaskSuite):
    def setup(self):
        a = da.random.normal(size=(4e6, 30e2), chunks=(2e4, 3e1))
        a = a.rechunk((int(1e4 / 10), int(30e2)))
        b = a.T.dot(a)
        self.dsk_rechunk_transpose = collections_to_dsk(b)

    def time_order04(self):
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
        self.dsk_linalg = collections_to_dsk(ey)

    def time_order_linalg_solve(self):
        order(self.dsk_linalg)


class OrderFullLayers(DaskSuite):
    def setup(self):
        self.dsk1 = fully_connected_layers(1, 50000)
        self.dsk2 = fully_connected_layers(2, 10000)
        self.dsk3 = fully_connected_layers(10, 1000)
        self.dsk4 = fully_connected_layers(100, 20)
        self.dsk5 = fully_connected_layers(500, 2)
        self.dsk6 = fully_connected_layers(9999, 1)
        self.dsk7 = fully_connected_layers(50000, 1)

    def time_order_full_1_50000(self):
        order(self.dsk1)

    def time_order_full_2_10000(self):
        order(self.dsk2)

    def time_order_full_10_1000(self):
        order(self.dsk3)

    def time_order_full_100_20(self):
        order(self.dsk4)

    def time_order_full_500_2(self):
        order(self.dsk5)

    def time_order_full_9999_1(self):
        order(self.dsk6)

    def time_order_full_50000_1(self):
        order(self.dsk7)


class OrderLinearWithDanglers(DaskSuite):
    def setup(self):
        self.dsk1 = {(0, 0): (f,)}
        for i in range(1, 10000):
            for j in range(2):
                self.dsk1[(i, j)] = (f, (i - 1, 0))

        self.dsk2 = {(0, 0): (f,)}
        for i in range(1, 5000):
            for j in range(5):
                self.dsk2[(i, j)] = (f, (i - 1, 0))

    def time_order_linear_danglers_2(self):
        order(self.dsk1)

    def time_order_linear_danglers_5(self):
        order(self.dsk2)


class OrderLinearFull(DaskSuite):
    def setup(self):
        # Although not a realistic DAG, this is cleverly constructed
        # to stress a current weakness of `order`.
        self.dsk = {0: (f,)}
        prev = (f, 0)
        for i in range(1, 1000):
            self.dsk[i] = prev
            prev += (i,)

    def time_order_linear_full(self):
        order(self.dsk)


class OrderManySubgraphs(DaskSuite):
    """This tests behavior when there are few or many disconnected subgraphs"""

    def setup(self):
        self.dsk1 = create_disconnected_subgraphs(1, 9999, 2)
        self.dsk2 = create_disconnected_subgraphs(3, 3333, 2)
        self.dsk3 = create_disconnected_subgraphs(10, 999, 2)
        self.dsk4 = create_disconnected_subgraphs(30, 303, 2)
        self.dsk5 = create_disconnected_subgraphs(100, 99, 2)
        self.dsk6 = create_disconnected_subgraphs(999, 10, 2)

    def time_order_many_subgraphs_1_9999(self):
        order(self.dsk1)

    def time_order_many_subgraphs_3_3333(self):
        order(self.dsk2)

    def time_order_many_subgraphs_10_999(self):
        order(self.dsk3)

    def time_order_many_subgraphs_30_303(self):
        order(self.dsk4)

    def time_order_many_subgraphs_100_99(self):
        order(self.dsk5)

    def time_order_many_subgraphs_999_10(self):
        order(self.dsk6)
