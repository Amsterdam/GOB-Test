from unittest import TestCase
from unittest.mock import patch, ANY

from gobtest.data_consistency.handler import data_consistency_test_handler, can_handle, GOBConfigException, \
    NotImplementedCatalogError, DataConsistencyTest


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
        mock_logger.configure.assert_called_with(msg, 'Data consistency test')

        mock_test.assert_called_with('the catalogue', 'the collection', 'the application')
        mock_test.return_value.run.assert_called_once()

        self.assertEqual({
            'header': {
                'catalogue': 'the catalogue',
                'collection': 'the collection',
                'entity': 'the collection',
                'application': 'the application',
                'timestamp': mock_datetime.datetime.utcnow.return_value.isoformat.return_value,
            },
            'summary': mock_logger.get_summary(),
        }, res)

        mock_logger.error.reset_mock()
        mock_test.side_effect = GOBConfigException
        res = data_consistency_test_handler(msg)
        mock_logger.error.assert_called()
        # Assert that a response is returned
        self.assertEqual(res, {'header': ANY, 'summary': ANY})

    @patch("gobtest.data_consistency.handler.DataConsistencyTest.run")
    @patch("gobtest.data_consistency.handler.logger")
    def test_rel_catalog(self, mock_logger, mock_run):
        msg = {
            'header': {
                'catalogue': 'rel',
                'collection': 'some coll',
                'application': 'some app'
            }
        }
        data_consistency_test_handler(msg)

        mock_logger.error.assert_called_with("Dataset test failed: Not implemented for the 'rel' catalog")
        mock_run.assert_not_called()

    @patch("gobtest.data_consistency.handler.DataConsistencyTest")
    def test_can_handle(self, mock_data_consistency_test):
        result = can_handle("cat", "col", "app")
        self.assertEqual(result, True)

        mock_data_consistency_test.side_effect = GOBConfigException
        result = can_handle("cat", "col", "app")
        self.assertIsNone(result)
