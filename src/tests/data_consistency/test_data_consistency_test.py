from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobtest.data_consistency.data_consistency_test import DataConsistencyTest


@patch("gobtest.data_consistency.data_consistency_test.get_import_definition")
@patch("gobtest.data_consistency.data_consistency_test.GOBModel")
class TestDataConsistencyTestInit(TestCase):
    """Tests only constructor

    """

    def test_init(self, mock_model, mock_get_import_definition):
        mock_model.return_value.get_collection.return_value = {'has_states': 'SioNo'}
        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
                'enrich': {
                    'enrich_column': {},
                }
            }
        }

        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual('THE ENTITY ID', instance.entity_id_field)
        self.assertEqual('SioNo', instance.has_states)

        # Check ignore columns is set and enrich_column is appended
        self.assertTrue(len(instance.ignore_columns) > 1)
        self.assertEqual('enrich_column', instance.ignore_columns[-1])

        self.assertEqual(mock_model.return_value.get_collection.return_value, instance.collection)

        mock_model.return_value.get_collection.assert_called_with('the cat', 'the col')
        mock_get_import_definition.assert_called_with('the cat', 'the col', 'the appl')


@patch("gobtest.data_consistency.data_consistency_test.get_import_definition", MagicMock())
@patch("gobtest.data_consistency.data_consistency_test.GOBModel", MagicMock())
class TestDataConsistencyTest(TestCase):

    @patch("gobtest.data_consistency.data_consistency_test.logger")
    @patch("gobtest.data_consistency.data_consistency_test.random")
    def test_run(self, mock_random, mock_logger):
        mock_random.randint.return_value = 2
        inst = DataConsistencyTest('cat', 'col', 'appl')
        inst.SAMPLE_SIZE = 0.25
        inst._connect = MagicMock()
        inst.has_states = False
        inst._get_row_id = lambda x: 'row id'
        inst._get_matching_gob_row = MagicMock(side_effect=lambda x: x * 2 if x * 2 % 10 != 0 else None)
        inst._validate_row = MagicMock(side_effect=lambda x, y: x % 6 == 0)
        inst._get_source_data = MagicMock(return_value=[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ])

        inst.run()
        mock_random.randint.assert_called_with(0, 3)
        inst._get_matching_gob_row.assert_has_calls([
            call(2),
            call(6),
            call(10),
        ])
        inst._validate_row.assert_has_calls([
            call(2, 4),
            call(6, 12),
        ])

        mock_logger.warning.assert_called_with('Row with id row id missing')
        mock_logger.error.assert_called_with('Have 1 missing rows in GOB, of 3 total rows.')
        mock_logger.info.assert_called_with('Completed data consistency test on 3 rows. '
                                            '1 rows contained errors. 1 rows could not be found.')

        # Check with higher threshold. Error should not be logged now.
        mock_logger.error.reset_mock()
        inst.MISSING_THRESHOLD = 0.5
        inst.run()
        mock_logger.error.assert_not_called()

    def test_geometry_to_wkt(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.analyse_db = MagicMock()
        inst.analyse_db.read.return_value = [['WKT VAL']]

        self.assertEqual('WKT VAL', inst._geometry_to_wkt('geoval'))
        inst.analyse_db.read.assert_called_with("SELECT ST_AsText('geoval'::geometry)")

    def test_normalise_wkt(self):
        inst = DataConsistencyTest('cat', 'col')
        test_cases = [
            ('POLYGON ((abcdefg', 'POLYGON((abcdefg'),
            ('POLYGON((12345.688 248024.02400', 'POLYGON((12345 248024'),
            ('POLYGON((11 22, 33 44', 'POLYGON((11 22,33 44'),
            ('POLYGON ((1244.24 8024.22, 248240.22 428025.55))', 'POLYGON((1244 8024,248240 428025))'),
        ]

        for arg, result in test_cases:
            self.assertEqual(result, inst._normalise_wkt(arg))

    @patch("gobtest.data_consistency.data_consistency_test.get_gob_type_from_info")
    def test_transform_source_row(self, mock_get_gob_type):
        # Bypass gob typesystem
        mock_get_gob_type.return_value.from_value = lambda x, **kwargs: type('Type', (), {'to_value': x})

        inst = DataConsistencyTest('cat', 'col')
        inst._normalise_wkt = lambda x: 'normalised(' + x + ')'
        inst.collection = {
            'all_fields': {
                'a': {
                    'type': '',
                },
                'b': {
                    'type': '',
                },
                'c': {
                    'type': '',
                },
                'd': {
                    'type': 'GOB.Geo.Geometry',
                }
            }
        }

        inst.import_definition = {
            'gob_mapping': {
                'a': {
                    'source_mapping': 'col a',
                },
                'b': {
                    'source_mapping': {
                        'b1': 'col b1',
                        'b2': 'col b2',
                    }
                },
                'c': {
                    'source_mapping': {
                        'c1': '=d',
                    }
                },
                'd': {
                    'source_mapping': 'col d',
                }
            }
        }

        source_row = {
            'col a': 'val a',
            'col b1': 'val b1',
            'col b2': 'val b2',
            'col d': 'val d',
        }

        expected_result = {
            'a': 'val a',
            'b_b1': 'val b1',
            'b_b2': 'val b2',
            'c_c1': 'd',
            'd': 'normalised(val d)'
        }

        self.assertEqual(expected_result, inst._transform_source_row(source_row))

    def test_transform_gob_row(self):
        inst = DataConsistencyTest('cat', 'col')
        inst._geometry_to_wkt = lambda x: 'wkt(' + x + ')'
        inst._normalise_wkt = lambda x: 'normalised(' + x + ')'
        inst.collection = {
            'all_fields': {
                'geofield': {
                    'type': 'GOB.Geo.Blah',
                }
            }
        }
        gob_row = {
            'geofield': 'geovalue',
            '_date_deleted': 'ignored',
        }

        self.assertEqual({
            'geofield': 'normalised(wkt(geovalue))'
        }, inst._transform_gob_row(gob_row))

    @patch("gobtest.data_consistency.data_consistency_test.logger")
    def test_validate_row(self, mock_logger):

        # First test if errors are logged correctly
        inst = DataConsistencyTest('cat', 'col')
        inst._transform_gob_row = lambda x: x
        inst._transform_source_row = lambda x: x

        inst._validate_row({}, {})
        mock_logger.error_assert_not_called()

        inst._validate_row({'a': 'aa'}, {'a': 'aa'})
        mock_logger.error_assert_not_called()

        inst._validate_row(
            {'a': 'aa'},
            {'a': 'ab'},
        )
        mock_logger.error.assert_called_once()

        mock_logger.error.reset_mock()
        inst._validate_row(
            {'a': 'aa'},
            {},
        )
        mock_logger.error.assert_called_with('Missing key a in GOB')

        mock_logger.error.reset_mock()
        inst._validate_row(
            {},
            {'a': 'aa'}
        )
        mock_logger.error.assert_called_with('Have unexpected keys left in GOB: a')

        # Check if the return value is correct based on the error logger

        # First 0 errors, last call contains 1 errors, so no success.
        mock_logger.get_errors.side_effect = [[], [1]]
        self.assertFalse(inst._validate_row({}, {}))

        # Errors are empty during both calls, which means no extra errors are logged
        mock_logger.get_errors.side_effect = [[], []]
        self.assertTrue(inst._validate_row({}, {}))

    def test_get_row_id(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.entity_id_field = 'the id'
        inst.import_definition = {
            'gob_mapping': {
                'the id': {
                    'source_mapping': 'idfield'
                }
            }
        }

        self.assertEqual('a', inst._get_row_id({'idfield': 'a'}))

    def test_get_matching_gob_row(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.has_states = True
        inst.analyse_db = MagicMock()
        inst.analyse_db.read.return_value = [{'the': 'row'}]
        inst.entity_id_field = 'ai die'

        inst.import_definition = {
            'gob_mapping': {
                'ai die': {
                    'source_mapping': 'idfield'
                },
                'volgnummer': {
                    'source_mapping': 'volgnr',
                }
            }
        }
        source_row = {
            'idfield': 'ID',
            'volgnr': 'SEQNR'
        }

        self.assertEqual({'the': 'row'}, inst._get_matching_gob_row(source_row))
        inst.analyse_db.read.assert_called_with("SELECT * FROM cat.col WHERE _id='ID' AND volgnummer='SEQNR'")

    def test_get_source_data(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.src_datastore = MagicMock()
        inst.source = {
            'query': ['a', 'b', 'c']
        }

        self.assertEqual(inst.src_datastore.query.return_value, inst._get_source_data())
        inst.src_datastore.query.assert_called_with('a\nb\nc')

    @patch("gobtest.data_consistency.data_consistency_test.DatastoreFactory")
    @patch("gobtest.data_consistency.data_consistency_test.get_datastore_config", lambda x: x + '_CONFIG')
    def test_connect(self, mock_factory):
        inst = DataConsistencyTest('cat', 'col')
        inst.source = {'application': 'app'}

        inst._connect()

        mock_factory.get_datastore.assert_has_calls([
            call('app_CONFIG', {}),
            call().connect(),
            call('GOBAnalyse_CONFIG'),
            call().connect(),
        ])
