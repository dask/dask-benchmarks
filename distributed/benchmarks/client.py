# -*- coding: utf-8 -*-
import copy

from dask import delayed
from distributed import Client, Worker, wait, LocalCluster
from distributed.utils_test import slowinc


class ClientSuite(object):

    def setup(self):
        self.client = Client()

    def time_trivial_tasks(self):
        """
        Measure scheduler and communication overhead by running
        a bunch of unrelated trivial tasks.
        """
        @delayed(pure=True)
        def inc(x):
            return x + 1

        L = [inc(i) for i in range(500)]
        total = delayed(sum, pure=True)(L)

        total.compute(scheduler=self.client)


class WorkerRestrictionsSuite(object):

    def setup(self):
        cluster = LocalCluster(n_workers=1, threads_per_worker=1,
                               resources={"resource": 1}, worker_class=Worker)
        spec = copy.deepcopy(cluster.new_worker_spec())
        del spec[1]['options']['resources']
        cluster.worker_spec.update(spec)
        cluster.scale(2)
        client = Client(cluster)

        self.client = client

    def teardown(self):
        self.client.close()

    def time_trivial_tasks_restrictions(self):
        client = self.client

        info = client.scheduler_info()
        workers = list(info['workers'])
        futures = client.map(slowinc, range(10),
                             delay=0.1, resources={"resource": 1})
        client.cluster.scale(len(workers) + 1)

        wait(futures)
        new_worker = client.cluster.workers[2]
        assert new_worker.available_resources == {'resource': 1}
