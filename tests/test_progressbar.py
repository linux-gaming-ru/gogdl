import itertools
import queue
import unittest
from unittest.mock import patch

from gogdl.dl.progressbar import ProgressBar


class ProgressBarTests(unittest.TestCase):
    def test_idle_wait_does_not_delay_queued_statistics(self) -> None:
        for source in (None, "network", "disk"):
            with self.subTest(source=source):
                network = queue.Queue()
                disk = queue.Queue()
                if source is not None:
                    target = network if source == "network" else disk
                    for _ in range(52):
                        target.put((512 * 1024, 1024 * 1024))
                progress = ProgressBar(128 * 1024 * 1024, network, disk)
                snapshots = []

                def report() -> None:
                    snapshots.append((progress.downloaded, progress.written_total))
                    if len(snapshots) > 1:
                        progress.completed = True

                def wait(_interval: float) -> None:
                    self.assertTrue(network.empty())
                    self.assertTrue(disk.empty())
                    progress.completed = True

                ticks = itertools.count(step=0.001)
                with patch("gogdl.dl.progressbar.time", side_effect=lambda: next(ticks)), \
                        patch("gogdl.dl.progressbar.sleep", side_effect=wait) as sleep, \
                        patch.object(progress, "print_progressbar", side_effect=report):
                    progress.loop()

                sleep.assert_called_once()
                expected = 26 * 1024 * 1024
                self.assertEqual(snapshots[-1], (
                    expected if source == "network" else 0,
                    expected if source == "disk" else 0,
                ))


if __name__ == "__main__":
    unittest.main()
