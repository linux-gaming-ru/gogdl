import copy
import hashlib
import queue
import sys
import unittest
import zlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

with patch.dict(sys.modules, {"gogdl_xdelta3": MagicMock()}):
    from gogdl.dl.workers.task_executor import Download, DownloadTask1, DownloadTask2


class DownloadWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = Download.__new__(Download)
        self.worker.secure_links = {"game": [{
            "url_format": "https://cdn.example{path}",
            "parameters": {"path": "/base"},
        }]}
        self.worker.session = MagicMock()
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


if __name__ == "__main__":
    unittest.main()
