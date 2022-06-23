from distutils.version import LooseVersion

import numpy as np
import pandas as pd

from dask.base import tokenize

from benchmarks.common import DaskSuite, rnd


class TokenizeBuiltins(DaskSuite):
    def setup(self):
        N = 10000
        self.obj = {i: ("tuple", ["list", i], {"set", i}, i) for i in range(N)}

    def time_tokenize(self):
        tokenize(self.obj)


class TokenizePandas(DaskSuite):
    params = [
        [
            "period",
            "datetime64[ns]",
            "datetime64[ns, CET]",
            "int",
            "category",
            "sparse",
            "Int",
        ],
        [True, False],
    ]
    if LooseVersion(pd.__version__) >= "1.0.0rc0":
        params[0].extend(["string", "boolean"])

    param_names = ["dtype", "as_series"]

    def setup(self, dtype, as_series):
        N = 10000
        if dtype == "period":
            array = pd.period_range("2000", periods=N).array
        elif dtype.startswith("datetime"):
            array = pd.date_range("2000", periods=N).astype(dtype)
        elif dtype == "int":
            array = np.arange(N)
        elif dtype == "Int":
            array = pd.array(np.arange(N), dtype="Int64")
        elif dtype == "category":
            array = pd.array(np.arange(10).repeat(N // 10), dtype="category")
        elif dtype == "boolean":
            array = pd.array(np.zeros(N, dtype="bool"), dtype="boolean")
        elif dtype == "string":
            array = pd.array(np.zeros(N, dtype="bool").astype(str), dtype="string")
        elif dtype == "sparse":
            array = pd.array(np.zeros(N, dtype="int"), dtype="Sparse")
        else:
            raise ValueError(dtype)
        if as_series:
            self.obj = pd.Series(array, name="name")
        else:
            self.obj = array

    def time_tokenize(self, dtype, as_series):
        tokenize(self.obj)


class TokenizeNumpy(DaskSuite):
    params = ["int", "float", "str", "bytes", "object"]
    param_names = ["dtype"]

    def setup(self, dtype):
        N = 1000000
        if dtype == "int":
            obj = rnd().randint(-1000, 1000 + 1, N).astype("i8")
        elif dtype == "float":
            obj = rnd().uniform(-1000, 1000 + 1, N).astype("f8")
        elif dtype in ("str", "bytes"):
            obj = rnd().choice(
                [
                    "Leslie",
                    "Ben",
                    "Jerry",
                    "Andy",
                    "April",
                    "Tom",
                    "Ann",
                    "Chris",
                    "Donna",
                    "Ron",
                ],
                N,
            )
            if dtype == "bytes":
                obj = obj.astype("S")
        elif dtype == "object":
            obj = rnd().randint(-1000, 1000 + 1, N).astype("object")
        self.obj = obj

    def time_tokenize(self, dtype):
        tokenize(self.obj)
