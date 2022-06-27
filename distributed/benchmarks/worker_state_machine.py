import random

from distributed.worker_state_machine import (
    AcquireReplicasEvent,
    GatherDep,
    GatherDepSuccessEvent,
    PauseEvent,
    UnpauseEvent,
    WorkerState,
)


class _EnsureCommunicating:
    param_names = ["n_workers", "n_tasks"]
    # Note: If you run _ensure_communicating twice in a row and there are enough workers
    # in flight to saturate total_out_connections, you trigger a fast exit code path
    params = [
        (5, 49, 50, 250),
        (100, 1000, 5000),
    ]

    def setup(self, n_workers, n_tasks):
        self.ws = WorkerState(
            address="1.0.0.1:1", total_out_connections=50, validate=False
        )
        workers = [f"1.0.0.1:{i}" for i in range(2, n_workers + 2)]
        rng = random.Random(42)

        who_has = {}
        for i in range(n_tasks):
            # 10% of the tasks are replicated on up to 50% of the workers
            if rng.random() >= 0.9:
                n_replicas = rng.randint(2, n_workers // 2)
            else:
                n_replicas = 1

            who_has[f"x{i}"] = rng.sample(workers, n_replicas)

        self.acquire_replicas = AcquireReplicasEvent(
            who_has=who_has,
            # Up to 4 tasks per GatherDep instruction (target_message_size = 50e6)
            nbytes={k: int(12e6) for k in who_has},
            stimulus_id="acquire-replicas",
        )

    def assert_instructions(
        self, instructions, expect_min: int, expect_max: int
    ) -> None:
        actual = sum(isinstance(instr, GatherDep) for instr in instructions)
        if actual < expect_min or actual > expect_max:
            raise AssertionError(
                f"Expected between {expect_min} and {expect_max} "
                f"GatherDep instructions; got {actual}"
            )


class PopulateDataNeeded(_EnsureCommunicating):
    """Create tasks in fetch state and push them into the data_needed heap"""

    def setup(self, n_workers, n_tasks):
        super().setup(n_workers, n_tasks)
        self.ws.handle_stimulus(PauseEvent(stimulus_id="pause"))
        self.ws.validate_state()

    def time_populate_data_needed(self, n_workers, n_tasks):
        # FIXME this breaks if you run asv without the --quick flag
        assert not self.ws.tasks
        instructions = self.ws.handle_stimulus(self.acquire_replicas)
        self.assert_instructions(instructions, 0, 0)
        assert not self.ws.in_flight_workers


class EnsureCommunicatingFromIdle(_EnsureCommunicating):
    """Call _ensure_communicating while no workers are in flight"""

    def setup(self, n_workers, n_tasks):
        super().setup(n_workers, n_tasks)
        self.ws.handle_stimulus(
            PauseEvent(stimulus_id="pause"),
            self.acquire_replicas,
        )
        self.ws.validate_state()

    def time_from_idle(self, n_workers, n_tasks):
        # FIXME this breaks if you run asv without the --quick flag
        assert not self.ws.in_flight_workers
        instructions = self.ws.handle_stimulus(UnpauseEvent(stimulus_id="unpause"))
        self.assert_instructions(instructions, 1, min(n_workers, 50))


class EnsureCommunicatingNoop(_EnsureCommunicating):
    """Call _ensure_communicating again while all workers are in flight"""

    def setup(self, n_workers, n_tasks):
        super().setup(n_workers, n_tasks)
        self.ws.handle_stimulus(self.acquire_replicas)
        self.ws.validate_state()

    def time_noop(self, n_workers, n_tasks):
        instructions = self.ws.handle_stimulus(UnpauseEvent(stimulus_id="unpause"))
        self.assert_instructions(instructions, 0, 0)


class EnsureCommunicatingOneWorker(_EnsureCommunicating):
    """While all workers are in flight, one returns data, so another batch of tasks can
    be fetched from the same worker
    """

    def setup(self, n_workers, n_tasks):
        super().setup(n_workers, n_tasks)
        instructions = self.ws.handle_stimulus(self.acquire_replicas)
        instr = next(i for i in instructions if isinstance(i, GatherDep))
        # Don't meter the transitions to memory
        instructions = self.ws.handle_stimulus(
            PauseEvent(stimulus_id="pause2"),
            GatherDepSuccessEvent(
                worker=instr.worker,
                data=dict.fromkeys(instr.to_gather),
                total_nbytes=instr.total_nbytes,
                stimulus_id="gather-dep-done",
            ),
        )
        self.assert_instructions(instructions, 0, 0)
        self.ws.validate_state()
        self.already_ran = False

    def time_one_worker(self, n_workers, n_tasks):
        # FIXME this breaks if you run asv without the --quick flag
        assert not self.already_ran
        self.already_ran = True
        instructions = self.ws.handle_stimulus(UnpauseEvent(stimulus_id="unpause2"))
        self.assert_instructions(instructions, 0 if n_tasks < n_workers * 5 else 1, 1)
