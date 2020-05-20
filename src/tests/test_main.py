from unittest import TestCase
from unittest.mock import patch

from gobtest.__main__ import SERVICEDEFINITION


class TestMain(TestCase):

    @patch("gobtest.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobtest import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Test", {"thread_per_service": True})
