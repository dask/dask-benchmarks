# -*- coding: utf-8 -*-
import copy

import time

from dask import delayed, config
from distributed import Client, Worker, wait, LocalCluster, stealing

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
    
    params = ([1,None], [0.01, 0.1,1,100])
    param_names = ["resource","steal_interval"]
    
    def setup(self,resource,steal_interval):
        config.set({"distributed.scheduler.work-stealing-interval":steal_interval})
        rdict = {"resource":resource} if resource else None
        cluster = LocalCluster(n_workers=1, threads_per_worker=1,
                               resources=rdict, worker_class=Worker)

        spec = copy.deepcopy(cluster.new_worker_spec())
        
        if resource:
            del spec[1]['options']['resources']
        cluster.worker_spec.update(spec)
        cluster.scale(2)
        client = Client(cluster)

        self.client = client

    def teardown(self,resource,steal_interval):
        self.client.close()

    def time_trivial_tasks(self,resource,steal_interval):
        """
        Benchmark measuring the improvment from allowing new workers
        to steal tasks with resource restrictions.
        """
        client = self.client
        rdict = {"resource":resource} if resource else None
        info = client.scheduler_info()
        workers = list(info['workers'])
        futures = client.map(slowinc, range(10),
                             delay=0.1, resources=rdict)
        client.cluster.scale(len(workers) + 1)

        wait(futures)
        new_worker = client.cluster.workers[2]
        if resource:
            assert new_worker.available_resources == rdict
        
        steal_plug = stealing.WorkStealing(client.cluster.scheduler)
        assert steal_plug._pc.callback_time == steal_interval
        # assert len(new_worker.task_state)

