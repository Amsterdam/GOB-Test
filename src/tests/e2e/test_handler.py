from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobtest.e2e.handler import (
    end_to_end_test_handler, end_to_end_check_handler, end_to_end_execute_workflow_handler, end_to_end_wait_handler
)


@patch("gobtest.e2e.handler.logger")
class TestHandler(TestCase):

    @patch("gobtest.e2e.handler.E2ETest")
    @patch("gobtest.e2e.handler.datetime")
    def test_end_to_end_test_handler(self, mock_datetime, mock_e2etest, mock_logger):
        self.assertEqual({
            'header': {
                'attribute': 'value',
                'process_id': '1.e2e_test',
                'timestamp': mock_datetime.datetime.utcnow.return_value.isoformat.return_value,
                'workflow': mock_e2etest.return_value.get_workflow.return_value,
            },
            'contents': ''
        }, end_to_end_test_handler({'header': {'attribute': 'value'}}))

        # Existing process id should be used
        res = end_to_end_test_handler({'header': {'process_id': 'existing'}})
        self.assertEqual('existing', res['header']['process_id'])

    @patch("gobtest.e2e.handler.E2ETest")
    def test_end_to_end_execute_workflow_handler(self, mock_e2etest, mock_logger):
        msg = {
            'header': {
                'execute': ['some', 'workflow'],
                'execute_process_id': 'process id to assign',
                'process_id': 'this process id',
            }
        }

        self.assertEqual({
            'header': {
                'execute': ['some', 'workflow'],
                'execute_process_id': 'process id to assign',
                'process_id': 'this process id',
            },
            'summary': mock_logger.get_summary(),
        }, end_to_end_execute_workflow_handler(msg))

        mock_e2etest().execute_workflow.assert_called_with(['some', 'workflow'], 'process id to assign')

    @patch("gobtest.e2e.handler.E2ETest")
    def test_end_to_en_wait_handler(self, mock_e2etest, mock_logger):
        msg = {
            'header': {
                'process_id': 'the process id',
                'wait_for_process_id': 'process to wait for',
                'seconds': 14904
            }
        }

        self.assertEqual({
            'header': {
                'process_id': 'the process id',
                'wait_for_process_id': 'process to wait for',
                'seconds': 14904
            },
            'summary': mock_logger.get_summary(),
        }, end_to_end_wait_handler(msg))

        mock_e2etest().wait.assert_called_with('process to wait for', 14904)

    @patch("gobtest.e2e.handler.E2ETest")
    def test_end_to_end_check_handler(self, mock_e2etest, mock_logger):
        self.assertEqual({
            'header': {
                'attribute': 'value',
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
                'process_id': 'the process id',
            },
            'summary': mock_logger.get_summary(),
        }, end_to_end_check_handler({
            'header': {
                'attribute': 'value',
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
                'process_id': 'the process id',
            }
        }))
        mock_e2etest.return_value.check.assert_called_with('endp', 'exp', 'desc')

        with self.assertRaises(AssertionError):
            end_to_end_check_handler(({
                'header': {}
            }))
