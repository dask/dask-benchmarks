from dask.core import get_dependencies
from dask.optimization import cull, fuse, inline, inline_functions

from .common import DaskSuite


def inc(x):
    return x + 1


def add(x, y):
    return x + y


class Cull(DaskSuite):
    def setup(self):
        # An embarrasingly parallel graph
        cols = 10000
        rows = 10
        dsk = {
            (r, c): 0 if r == 0 else (inc, (r - 1, c))
            for r in range(rows)
            for c in range(cols)
        }
        # Cull away half the rows
        keys = [(rows - 1, c) for c in range(0, cols, 2)]
        self.dsk = dsk
        self.keys = keys

    def time_cull(self):
        cull(self.dsk, self.keys)


class Fuse(DaskSuite):
    params = ["diamond",
              "linear"]

    def setup(self, kind):
        dsk, keys, kwargs = getattr(self, "setup_" + kind)()
        self.dsk = dsk
        self.keys = keys
        self.extra_kwargs = kwargs
        self.deps = {k: get_dependencies(dsk, k, as_list=True) for k in dsk}

    def setup_linear(self):
        # An embarrasingly parallel graph
        cols = 2000
        rows = 10
        dsk = {
            (r, c): 0 if r == 0 else (inc, (r - 1, c))
            for r in range(rows)
            for c in range(cols)
        }
        keys = [(rows - 1, c) for c in range(0, cols, 2)]
        return dsk, keys, {}

    def setup_diamond(self):
        # Parallel stacks of diamonds
        cols = 2000
        ndiamonds = 3
        width = 2
        height = 2
        rows = ndiamonds * (height + 1) + 1
        dsk = {("x", 0, c): 0 for c in range(cols)}
        x = "x"
        for r in range(1, rows):
            if not (r - 1) % (height + 1):
                # Start of a diamond
                update = {
                    ("add", r, c, w): (add, (x, r - 1, c), w)
                    for w in range(width) for c in range(cols)
                }
                x = "add"
            elif r % (height + 1):
                # In a diamond
                update = {
                    ("inc", r, c, w): (inc, (x, r - 1, c, w))
                    for w in range(width) for c in range(cols)
                }
                x = "inc"
            else:
                # End of a diamond
                update = {
                    ("sum", r, c): (sum, [(x, r - 1, c, w) for w in range(width)])
                    for c in range(cols)
                }
                x = "sum"
            dsk.update(update)
        keys = list(update)
        return dsk, keys, {"ave_width": width + 1}

    def time_fuse(self, kind):
        fuse(self.dsk, self.keys, self.deps, **self.extra_kwargs)


class Inline(DaskSuite):
    def setup(self):
        cols = 1000
        rows = 10
        width = 3
        height = 2
        dsk = {}
        inline_keys = set()
        for r in range(rows):
            if r == 0:
                update = {("x", r, c): 0 for c in range(cols)}
                x = "x"
            elif not (r - 1) % (height + 1):
                # Start of a diamond
                update = {
                    ("add", r, c, w): (add, (x, r - 1, c), w)
                    for w in range(width) for c in range(cols)
                }
                x = "add"
            elif r % (height + 1):
                # In a diamond
                update = {
                    ("inc", r, c, w): (inc, (x, r - 1, c, w))
                    for w in range(width) for c in range(cols)
                }
                inline_keys.update(update)
                x = "inc"
            else:
                # End of a diamond
                update = {
                    ("sum", r, c): (sum, [(x, r - 1, c, w) for w in range(width)])
                    for c in range(cols)
                }
                x = "sum"
            dsk.update(update)
        keys = list(update)
        self.dsk = dsk
        self.keys = keys
        self.inline_keys = inline_keys
        self.deps = {k: get_dependencies(dsk, k, as_list=False) for k in dsk}

    def time_inline_constants(self):
        inline(self.dsk, inline_constants=True, dependencies=self.deps)

    def time_inline_keys(self):
        inline(self.dsk, keys=self.inline_keys, dependencies=self.deps)

    def time_inline_functions(self):
        inline_functions(
            self.dsk, self.keys, fast_functions=[inc], dependencies=self.deps
        )
