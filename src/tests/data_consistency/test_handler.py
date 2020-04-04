from unittest import TestCase
from unittest.mock import patch

from gobtest.data_consistency.handler import data_consistency_test_handler


class TestDataConsistencyTestHandler(TestCase):

    @patch("gobtest.data_consistency.handler.datetime")
    @patch("gobtest.data_consistency.handler.logger")
    @patch("gobtest.data_consistency.handler.DataConsistencyTest")
    def test_data_consistency_test_handler(self, mock_test, mock_logger, mock_datetime):
        msg = {
            'header': {
                'catalogue': 'the catalogue',
                'collection': 'the collection',
                'application': 'the application',
            }
        }
        res = data_consistency_test_handler(msg)

        # Assert logger configured
        mock_logger.configure.assert_called_with(msg, 'Data consistency E2E test')

        mock_test.assert_called_with('the catalogue', 'the collection', 'the application')
        mock_test.return_value.run.assert_called_once()

        self.assertEqual({
            'header': {
                'catalogue': 'the catalogue',
                'collection': 'the collection',
                'application': 'the application',
                'timestamp': mock_datetime.datetime.utcnow.return_value.isoformat.return_value,
            },
            'summary': {
                'warnings': mock_logger.get_warnings.return_value,
                'errors': mock_logger.get_errors.return_value,
            }
        }, res)
