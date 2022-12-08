from unittest import TestCase
from unittest.mock import patch

from gobtest.__main__ import SERVICEDEFINITION, DATA_CONSISTENCY_TEST, on_events_listener


class TestMain(TestCase):

    @patch("gobtest.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobtest import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Test", {"thread_per_service": True})

    @patch("gobtest.__main__.start_workflow")
    @patch("gobtest.__main__.can_handle")
    def test_on_events_listener(self, mock_can_handle, mock_start_workflow):
        msg = {
            'type': 'dump',
            'contents': {'collection': 'SOME COLL', 'catalog': 'SOME CAT'},
            'header': {
                'catalogue': 'SOME CAT',
                'collection': 'SOME COLL',
                'application': 'SOME APP',
                'process_id': 'PROCESS ID'
            }
        }

        mock_can_handle.return_value = False
        on_events_listener(msg)

        mock_start_workflow.assert_not_called()

        mock_can_handle.return_value = True
        on_events_listener(msg)

        mock_can_handle.assert_called_with(**{
            'catalogue': 'SOME CAT',
            'collection': 'SOME COLL',
            'application': 'SOME APP',
        })

        mock_start_workflow.assert_called_with({'workflow_name': DATA_CONSISTENCY_TEST}, {
            'catalogue': 'SOME CAT',
            'collection': 'SOME COLL',
            'application': 'SOME APP',
            'process_id': 'PROCESS ID',
        })
