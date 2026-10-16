import copy
import hashlib
import queue
import sys
import unittest
import zlib
import requests
from types import SimpleNamespace
from threading import Lock
from unittest.mock import MagicMock, patch

with patch.dict(sys.modules, {"gogdl_xdelta3": MagicMock()}):
    from gogdl.dl.workers import task_executor as worker_module
    from gogdl.dl.workers.task_executor import Download, DownloadTask1, DownloadTask2, FailReason


class DownloadWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = Download.__new__(Download)
        self.worker.secure_links = {"game": [{
            "url_format": "https://cdn.example{path}",
            "parameters": {"path": "/base"},
        }]}
        self.worker.session = MagicMock()
        self.worker.cdn_selector = worker_module.CDNSelector({}, Lock())
        self.worker.results_queue = queue.Queue()
        self.worker.speed_queue = queue.Queue()
        self.worker.shared_memory = SimpleNamespace(buf=bytearray(4096))

    def test_repeated_url_building_preserves_secure_links(self) -> None:
        urls = self.worker.secure_links["game"]
        original = copy.deepcopy(urls)
        task = SimpleNamespace(product_id="game", compressed_sum="abcdef")
        expected = "https://cdn.example/base/ab/cd/abcdef"
        self.assertEqual(self.worker._get_download_url_v2(task, urls, 0), expected)
        self.assertEqual(self.worker._get_download_url_v2(task, urls, 0), expected)
        self.assertEqual(urls, original)
        self.assertEqual(self.worker._get_download_url_v1(urls), "https://cdn.example/base/main.bin")
        self.assertEqual(self.worker._get_download_url_v1(urls), "https://cdn.example/base/main.bin")
        self.assertEqual(urls, original)

    def test_v2_writes_decompressed_bytes_and_reports_network_bytes(self) -> None:
        payload = b"game content" * 100
        compressed = zlib.compress(payload)
        response = self.worker.session.get.return_value
        response.iter_content.return_value = [compressed[:8], compressed[8:]]
        task = DownloadTask2("game", hashlib.md5(compressed).hexdigest(), SimpleNamespace(offset=3))
        self.worker.v2(task)
        result = self.worker.results_queue.get_nowait()
        self.assertTrue(result.success)
        self.assertEqual(result.download_size, len(compressed))
        self.assertEqual(result.decompressed_size, len(payload))
        self.assertEqual(self.worker.shared_memory.buf[3:3 + len(payload)], payload)

    def test_v1_writes_all_response_chunks(self) -> None:
        response = self.worker.session.get.return_value
        response.iter_content.return_value = [b"abc", b"def"]
        task = DownloadTask1("game", 0, 6, "checksum_0", SimpleNamespace(offset=2))
        self.worker.v1(task)
        result = self.worker.results_queue.get_nowait()
        self.assertTrue(result.success)
        self.assertEqual(result.download_size, 6)
        self.assertEqual(self.worker.shared_memory.buf[2:8], b"abcdef")

    def test_http_auth_errors_are_reported_without_retrying(self) -> None:
        for generation in (1, 2):
            for status in (401, 403):
                with self.subTest(generation=generation, status=status):
                    self.worker.session.reset_mock()
                    response = requests.Response()
                    response.status_code = status
                    response.raw = MagicMock()
                    self.worker.session.get.return_value = response
                    segment = SimpleNamespace(offset=0)
                    task = (
                        DownloadTask1("game", 0, 6, "checksum_0", segment)
                        if generation == 1 else DownloadTask2("game", "checksum", segment)
                    )
                    with patch.object(worker_module.time, "sleep"):
                        getattr(self.worker, f"v{generation}")(task)
                    result = self.worker.results_queue.get_nowait()
                    self.assertEqual(result.fail_reason, FailReason.UNAUTHORIZED)
                    self.worker.session.get.assert_called_once()

    def test_downloads_sample_mirrors_then_use_the_faster_one(self) -> None:
        for generation in (1, 2):
            with self.subTest(generation=generation):
                self.worker.cdn_selector.state.clear()
                self.worker.session.reset_mock()
                self.worker.secure_links["game"] = [
                    {"url_format": f"https://{host}.example{{path}}", "parameters": {"path": "/base"}}
                    for host in ("slow", "fast")
                ]
                payload = b"game content"
                data = zlib.compress(payload) if generation == 2 else payload
                response = self.worker.session.get.return_value
                clock = [0.0]
                delays = iter([2.0, 0.25, 0.25])

                def receive(_size):
                    clock[0] += next(delays)
                    return [data]

                response.iter_content.side_effect = receive
                segment = SimpleNamespace(offset=0)
                task = (
                    DownloadTask2("game", hashlib.md5(data).hexdigest(), segment)
                    if generation == 2 else DownloadTask1("game", 0, len(data), "chunk", segment)
                )
                with patch.object(worker_module.time, "monotonic", side_effect=lambda: clock[0]):
                    for _ in range(3):
                        getattr(self.worker, f"v{generation}")(task)
                        self.assertTrue(self.worker.results_queue.get_nowait().success)
                hosts = [worker_module.urlsplit(call.args[0]).netloc for call in self.worker.session.get.call_args_list]
                self.assertEqual(hosts, ["slow.example", "fast.example", "fast.example"])
                self.assertEqual(response.close.call_count, 3)

    def test_failed_mirror_falls_back_without_becoming_preferred(self) -> None:
        self.worker.secure_links["game"] = [
            {"url_format": f"https://{host}.example{{path}}", "parameters": {"path": "/base"}}
            for host in ("broken", "working")
        ]
        data = zlib.compress(b"game content")
        response = MagicMock()
        response.iter_content.return_value = [data]
        self.worker.session.get.side_effect = [requests.ConnectionError, response]
        task = DownloadTask2("game", hashlib.md5(data).hexdigest(), SimpleNamespace(offset=0))
        self.worker.v2(task)
        self.assertTrue(self.worker.results_queue.get_nowait().success)
        stats = self.worker.cdn_selector.state["broken.example"]
        self.assertGreater(stats["retry_after"], worker_module.time.monotonic())
        self.assertEqual(stats["active"], 0)
        self.assertEqual(response.close.call_count, 1)

    def test_direct_installer_url_still_downloads(self) -> None:
        self.worker.secure_links["game"] = "https://installer.example/game"
        self.worker.session.get.return_value.iter_content.return_value = [b"abc"]
        task = DownloadTask1("game", 0, 3, "chunk", SimpleNamespace(offset=0))
        self.worker.v1(task)
        self.assertTrue(self.worker.results_queue.get_nowait().success)
        self.assertEqual(self.worker.session.get.call_args.args[0], "https://installer.example/game")


if __name__ == "__main__":
    unittest.main()
