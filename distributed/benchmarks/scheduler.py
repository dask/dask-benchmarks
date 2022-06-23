import random

from dask import compute, delayed
from distributed import Client, wait


class SchedulerComputeDepsInMemory:
    def setup(self):
        self.client = Client()

        # Generate 10 indepdent tasks
        x = [delayed(random.random)() for _ in range(10)]
        # Generate lots of interrelated dependent tasks
        n = 200
        for _ in range(10, n):
            random_subset = [random.choice(x) for _ in range(5)]
            random_max = delayed(max)(random_subset)
            x.append(random_max)

        # Persist tasks into distributed memory and wait to finish
        y = self.client.persist(x)
        wait(y)

        self.x = x

    def teardown(self):
        self.client.close()

    def time_compute_deps_already_in_memory(self):
        """
        Measure compute time when dependent tasks are already in memory.
        xref https://github.com/dask/distributed/pull/3293
        """
        compute(*self.x, scheduler=self.client)
