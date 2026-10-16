import sys
import unittest
from threading import Lock
from unittest.mock import MagicMock, patch

with patch.dict(sys.modules, {"gogdl_xdelta3": MagicMock()}):
    from gogdl.dl.workers import task_executor as worker_module


class CDNSelectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = {}
        self.lock = Lock()
        self.selector = worker_module.CDNSelector(self.state, self.lock)
        self.urls = ["https://slow.example/chunk", "https://fast.example/chunk"]
        self.clock = 0.0
        clock = patch.object(worker_module.time, "monotonic", side_effect=lambda: self.clock)
        clock.start()
        self.addCleanup(clock.stop)

    def sample(self, index: int, size: int, seconds: float) -> None:
        started = self.clock
        self.clock += seconds
        self.selector.complete(self.urls[index], started, size)

    def measure_both(self) -> None:
        self.assertEqual(self.selector.select(self.urls, set()), 0)
        self.sample(0, 100, 2)
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 100, 1)

    def test_workers_coordinate_initial_probes(self) -> None:
        another = worker_module.CDNSelector(self.state, self.lock)
        self.assertEqual(self.selector.select(self.urls, set()), 0)
        self.assertEqual(another.select(self.urls, set()), 1)
        self.sample(0, 100, 2)
        self.sample(1, 100, 1)
        self.assertEqual(another.select(self.urls, set()), 1)

    def test_speed_drop_switches_preferred_mirror(self) -> None:
        self.measure_both()
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 10, 1)
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 10, 1)
        self.assertEqual(self.selector.select(self.urls, set()), 0)

    def test_unused_mirror_is_rechecked_after_interval(self) -> None:
        self.measure_both()
        self.clock += worker_module.CDN_RECHECK_INTERVAL
        self.assertEqual(self.selector.select(self.urls, set()), 0)
        self.sample(0, 1000, 1)
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 100, 1)
        self.assertEqual(self.selector.select(self.urls, set()), 0)

    def test_failure_cooldown_then_recovery(self) -> None:
        self.selector.select(self.urls, set())
        self.selector.complete(self.urls[0], self.clock, None)
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 100, 1)
        self.assertEqual(self.selector.select(self.urls, set()), 1)
        self.sample(1, 100, 1)
        self.clock += worker_module.CDN_FAILURE_COOLDOWN
        self.assertEqual(self.selector.select(self.urls, set()), 0)

    def test_auth_errors_release_probe_without_penalizing_host(self) -> None:
        self.selector.select(self.urls, set())
        self.selector.complete(self.urls[0], self.clock, 0)
        self.assertEqual(self.state["slow.example"]["active"], 0)
        self.assertEqual(self.state["slow.example"]["retry_after"], 0)

    def test_retry_exclusions_and_all_failed_mirrors(self) -> None:
        self.assertEqual(self.selector.select(self.urls, {0}), 1)
        self.selector.complete(self.urls[1], self.clock, None)
        self.assertEqual(self.selector.select(self.urls, {1}), 0)
        self.selector.complete(self.urls[0], self.clock, None)
        self.assertIn(self.selector.select(self.urls, set()), (0, 1))


if __name__ == "__main__":
    unittest.main()
