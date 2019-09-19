# -*- coding: utf-8 -*-
from distributed import Client

from dask import delayed


class ClientSuite(object):

    def setup(self):
        self.client = Client()
        """
        self.loop = IOLoop()
        self.scheduler = Scheduler()
        self.scheduler.start(0)
        self.workers = [Worker(self.scheduler.ip, self.scheduler.port,
                               loop=loop) for i in range(4)]
        self.client = Client(self.scheduler.address, loop=loop, start=False)

        @gen.coroutine
        def start():
            yield [w._start(0) for w in self.workers]
            yield self.client._start()

        loop.run_sync(start)
        """

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
