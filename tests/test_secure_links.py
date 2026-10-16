import unittest
from unittest.mock import MagicMock, patch

import requests
from gogdl.dl import dl_utils
from gogdl.api import ApiHandler


class SecureLinkTests(unittest.TestCase):
    def test_request_failures_have_a_retry_limit(self) -> None:
        api = MagicMock()
        with patch.object(dl_utils.requests, "get", side_effect=requests.ConnectionError) as get, \
                patch.object(dl_utils.time, "sleep"):
            with self.assertRaises(requests.ConnectionError):
                dl_utils.get_secure_link(api, "/", "game")
        self.assertEqual(get.call_count, 5)

    def test_http_failures_close_every_response(self) -> None:
        response = MagicMock(status_code=503)
        with patch.object(dl_utils.requests, "get", return_value=response) as get, \
                patch.object(dl_utils.time, "sleep"):
            with self.assertRaises(requests.HTTPError):
                dl_utils.get_secure_link(MagicMock(), "/", "game")
        self.assertEqual(get.call_count, 5)
        self.assertEqual(response.close.call_count, 5)

    def test_keyboard_interrupt_is_not_retried(self) -> None:
        with patch.object(dl_utils.requests, "get", side_effect=KeyboardInterrupt) as get:
            with self.assertRaises(KeyboardInterrupt):
                dl_utils.get_secure_link(MagicMock(), "/", "game")
        get.assert_called_once()

    def test_retry_preserves_patch_root(self) -> None:
        response = MagicMock(status_code=200)
        response.json.return_value = {"urls": ["fresh"]}
        with patch.object(dl_utils.requests, "get", side_effect=[requests.Timeout, response]) as get, \
                patch.object(dl_utils.time, "sleep"):
            links = dl_utils.get_secure_link(MagicMock(), "/", "game", root="/patches/store")
        self.assertEqual(links, ["fresh"])
        self.assertEqual(get.call_count, 2)
        self.assertTrue(all("root=/patches/store" in call.args[0] for call in get.call_args_list))
        response.close.assert_called_once()

    def test_refresh_failure_releases_product_for_next_attempt(self) -> None:
        api = ApiHandler(MagicMock(get_credentials=MagicMock(return_value=None)))
        self.addCleanup(api.session.close)
        with patch.object(api, "_ApiHandler__obtain_secure_link", side_effect=requests.Timeout):
            with self.assertRaises(requests.Timeout):
                api.get_new_secure_link("game")
        self.assertEqual(api.working_on_ids, [])


if __name__ == "__main__":
    unittest.main()
