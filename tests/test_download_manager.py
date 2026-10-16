import sys
import tempfile
import unittest
from collections import deque
from pathlib import Path
from threading import Condition
from unittest.mock import MagicMock, patch

with patch.dict(sys.modules, {"gogdl_xdelta3": MagicMock()}):
    from gogdl.dl.managers import task_executor as manager_module

worker = manager_module.task_executor
generic = manager_module.generic


class DownloadManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.manager = manager_module.ExecutingManager(
            MagicMock(), 1, temporary.name, None, generic.BaseDiff(), {},
        )
        self.manager.fatal_error = False
        self.manager.cdn_selector = MagicMock()
        self.manager.shared_secure_links = {"game": ["expired"]}
        self.manager.download_queue = MagicMock()
        self.manager.download_res_queue = MagicMock()
        self.manager.writer_queue = MagicMock()
        self.manager.tasks = deque([generic.ChunkTask("game", 0, "chunk", "sum", 3, 3)])
        self.task = worker.DownloadTask2("game", "chunk", generic.MemorySegment(0, 3))

    def test_repeated_chunk_failures_stop_the_download(self) -> None:
        result = worker.DownloadTaskResult(False, worker.FailReason.CHECKSUM, self.task)
        calls = 0

        def get_result(**_kwargs):
            nonlocal calls
            calls += 1
            if calls > 3:
                self.manager.running = False
            return result

        self.manager.download_res_queue.get.side_effect = get_result
        self.manager.process_task_results(Condition())
        self.assertTrue(self.manager.fatal_error)
        self.assertFalse(self.manager.running)
        self.assertEqual(calls, 3)
        self.assertEqual(self.manager.download_queue.put.call_count, 2)

    def test_expired_link_is_refreshed_before_retry(self) -> None:
        self.manager.api_handler.get_new_secure_link.return_value = ["fresh"]
        self.manager.download_res_queue.get.side_effect = [
            worker.DownloadTaskResult(False, worker.FailReason.UNAUTHORIZED, self.task),
            worker.DownloadTaskResult(True, None, self.task),
        ]

        def retry(task, **_kwargs):
            self.assertIs(task, self.task)
            self.assertEqual(self.manager.shared_secure_links["game"], ["fresh"])

        self.manager.download_queue.put.side_effect = retry
        self.manager.process_task_results(Condition())
        self.assertFalse(self.manager.fatal_error)
        self.manager.api_handler.get_new_secure_link.assert_called_once_with(
            "game", generation=2, root=None,
        )
        self.manager.writer_queue.put.assert_called_once()

    def test_failed_refresh_stops_without_resubmitting(self) -> None:
        failure = worker.DownloadTaskResult(False, worker.FailReason.UNAUTHORIZED, self.task)
        self.manager.download_res_queue.get.return_value = failure
        self.manager.api_handler.get_new_secure_link.side_effect = OSError("offline")
        self.manager.download_queue.put.side_effect = lambda *_args, **_kwargs: setattr(self.manager, "running", False)
        self.manager.process_task_results(Condition())
        self.assertTrue(self.manager.fatal_error)
        self.manager.download_queue.put.assert_not_called()

    def test_patch_refresh_preserves_product_and_root(self) -> None:
        self.task.product_id = "game_patch"
        self.manager.shared_secure_links["game_patch"] = ["expired"]
        self.manager.api_handler.get_new_secure_link.return_value = ["fresh patch"]
        self.manager._refresh_download_link(self.task)
        self.manager.api_handler.get_new_secure_link.assert_called_once_with(
            "game", generation=2, root="/patches/store",
        )
        self.assertEqual(self.manager.shared_secure_links["game_patch"], ["fresh patch"])

    def test_gen1_refresh_uses_gen1_endpoint(self) -> None:
        task = worker.DownloadTask1("game", 0, 3, "chunk", generic.MemorySegment(0, 3))
        self.manager._refresh_download_link(task)
        self.manager.api_handler.get_new_secure_link.assert_called_once_with(
            "game", generation=1, root=None,
        )

    def test_redist_refresh_uses_dependency_links(self) -> None:
        self.task.product_id = "redist"
        with patch.object(manager_module.dl_utils, "get_dependency_link", return_value=["fresh redist"]):
            self.manager._refresh_download_link(self.task)
        self.assertEqual(self.manager.shared_secure_links["redist"], ["fresh redist"])
        self.manager.api_handler.get_new_secure_link.assert_not_called()

    def test_installer_expiry_and_empty_links_are_errors(self) -> None:
        self.manager.shared_secure_links["game"] = "https://example.com/installer"
        with self.assertRaises(OSError):
            self.manager._refresh_download_link(self.task)
        self.manager.shared_secure_links["game"] = ["expired"]
        self.manager.api_handler.get_new_secure_link.return_value = []
        with self.assertRaises(OSError):
            self.manager._refresh_download_link(self.task)

    def test_run_raises_after_fatal_error_cleanup(self) -> None:
        self.manager.biggest_chunk = 1
        self.manager.download_speed_updates = MagicMock()
        self.manager.writer_speed_updates = MagicMock()
        self.manager.writer_res_queue = MagicMock()

        def cleanup() -> None:
            self.manager.fatal_error = True

        with patch.object(manager_module, "SharedMemory", return_value=MagicMock(size=0)), \
                patch.object(manager_module, "Thread"), \
                patch.object(manager_module, "ProgressBar"), \
                patch.object(worker, "Download"), patch.object(worker, "Writer"), \
                patch.object(manager_module.signal, "signal"), \
                patch.object(self.manager, "shutdown", side_effect=cleanup) as shutdown:
            with self.assertRaises(OSError):
                self.manager.run()
        shutdown.assert_called_once()

    def test_shutdown_preserves_resume_on_failure_and_joins_threads(self) -> None:
        for failed in (False, True):
            with self.subTest(failed=failed):
                resume = Path(self.manager.resume_file)
                resume.write_text("completed-file\n")
                self.manager.fatal_error = failed
                self.manager.progress = MagicMock(is_alive=MagicMock(return_value=False))
                self.manager.download_speed_updates = MagicMock()
                self.manager.writer_speed_updates = MagicMock()
                self.manager.download_speed_updates.get_nowait.side_effect = manager_module.Empty
                self.manager.writer_speed_updates.get_nowait.side_effect = manager_module.Empty
                self.manager.writer_res_queue = MagicMock()
                self.manager.writer_worker = MagicMock()
                self.manager.manager = MagicMock()
                thread = MagicMock()
                self.manager.threads = [thread]

                def close() -> None:
                    thread.join.assert_called_once()

                self.manager.writer_queue.close.side_effect = close
                self.manager.shutdown()
                self.assertEqual(resume.exists(), failed)


if __name__ == "__main__":
    unittest.main()
