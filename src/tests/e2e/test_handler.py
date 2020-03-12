from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobtest.e2e.handler import end_to_end_test_handler, end_to_end_check_handler


@patch("gobtest.e2e.handler.logger")
class TestHandler(TestCase):

    @patch("gobtest.e2e.handler.E2ETest")
    @patch("gobtest.e2e.handler.datetime")
    def test_end_to_end_test_handler(self, mock_datetime, mock_e2etest, mock_logger):
        self.assertEqual({
            'header': {
                'attribute': 'value',
                'timestamp': mock_datetime.datetime.utcnow.return_value.isoformat.return_value,
                'workflow': mock_e2etest.return_value.get_workflow.return_value,
            },
            'contents': ''
        }, end_to_end_test_handler({'header': {'attribute': 'value'}}))

    @patch("gobtest.e2e.handler.E2ETest")
    def test_end_to_end_check_handler(self, mock_e2etest, mock_logger):
        self.assertEqual({
            'header': {
                'attribute': 'value',
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
            },
            'summary': {
                'warnings': mock_logger.get_warnings.return_value,
                'errors': mock_logger.get_errors.return_value,
            }
        }, end_to_end_check_handler({
            'header': {
                'attribute': 'value',
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
            }
        }))
        mock_e2etest.return_value.check.assert_called_with('endp', 'exp', 'desc')

        with self.assertRaises(AssertionError):
            end_to_end_check_handler(({
                'header': {}
            }))

