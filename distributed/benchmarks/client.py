# -*- coding: utf-8 -*-
import copy

import time

from dask import delayed
from distributed import Client, Worker, wait, LocalCluster


def slowinc(x, delay=0.02):
    time.sleep(delay)
    return x + 1


class ClientSuite(object):

    def setup(self):
        self.client = Client()

    def teardown(self):
        self.client.close()

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

    params = [{"resource":1},None]

    def setup(self,resource):
        cluster = LocalCluster(n_workers=1, threads_per_worker=1,
                               resources=resource, worker_class=Worker)
        spec = copy.deepcopy(cluster.new_worker_spec())
        
        if resource:
            del spec[1]['options']['resources']
        cluster.worker_spec.update(spec)
        cluster.scale(2)
        client = Client(cluster)

        self.client = client

    def teardown(self,resource):
        self.client.close()

    def time_trivial_tasks(self,resource):
        """
        Benchmark measuring the improvment from allowing new workers
        to steal tasks with resource restrictions.
        """
        client = self.client

        info = client.scheduler_info()
        workers = list(info['workers'])
        futures = client.map(slowinc, range(10),
                             delay=0.1, resources=resource)
        client.cluster.scale(len(workers) + 1)

        wait(futures)
        new_worker = client.cluster.workers[2]
        # assert new_worker.available_resources == {'resource': 1}
        # assert len(new_worker.task_state)
