import copy
import queue
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

with patch.dict(sys.modules, {"gogdl_xdelta3": MagicMock()}):
    from gogdl.dl.workers.task_executor import Download


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



if __name__ == "__main__":
    unittest.main()
