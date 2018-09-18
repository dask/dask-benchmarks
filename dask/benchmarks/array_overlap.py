from dask import array as da
from dask.array.overlap import map_overlap

from .common import DaskSuite, rnd

class MapOverlap(DaskSuite):
    params = [[(100,) * 3, (50, 512, 512)],
              ['reflect', 'periodic', 'nearest', 'none'] ]
    param_names = ['shape', 'boundary']


    def setup(self, shape, boundary):
        arr = da.ones(shape, chunks=[s//2 for s in shape])
        self.arr = arr.persist()

    def time_map_overlap(self, shape, boundary):
        map_overlap(self.arr, lambda x: x,
            depth=1, boundary=boundary, trim=True).compute()
