from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import datetime

from gobtest.data_consistency.data_consistency_test import DataConsistencyTest, GOBException, GOBTypeException, Reference, FIELD, NotImplementedCatalogError
from gobcore.typesystem.gob_types import ManyReference
from gobcore.typesystem import GOB, GEO

@patch("gobtest.data_consistency.data_consistency_test.get_import_definition")
@patch("gobtest.data_consistency.data_consistency_test.GOBModel")
class TestDataConsistencyTestInit(TestCase):
    """Tests only constructor

    """

    def test_init(self, mock_model, mock_get_import_definition):
        mock_model.return_value.get_collection.return_value = {'has_states': 'SioNo', 'references': [], 'attributes': {}}
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
        self.assertTrue(FIELD.SEQNR not in instance.ignore_columns)

        self.assertEqual(mock_model.return_value.get_collection.return_value, instance.collection)

        mock_model.return_value.get_collection.assert_called_with('the cat', 'the col')
        mock_get_import_definition.assert_called_with('the cat', 'the col', 'the appl')

        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
                'merge': {}
            }
        }
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertTrue(instance.is_merged)
        self.assertTrue(FIELD.SEQNR in instance.ignore_columns)

    def test_init_rel(self, mock_model, mock_get_import_definition):
        with self.assertRaises(NotImplementedCatalogError):
            DataConsistencyTest('rel', 'some coll', 'some app')

    def test_init_skip_secure_attributes(self, mock_model, mock_get_import_definition):
        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
            },
            'gob_mapping': {}
        }

        mock_attributes = {
            'a': {
                'type': 'GOB.SecureString'
            },
            'b': {
                'type': 'GOB.Reference',
                'secure': 'any secure reference'
            },
            'c': {
                'type': 'GOB.String'
            },
            'd': {
                'type': 'GOB.Reference'
            },
        }
        mock_model.return_value.get_collection.return_value = {'attributes': mock_attributes}
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual(instance.ignore_columns, instance.default_ignore_columns + ['a', 'b', 'b_bronwaarde'])

    def test_init_skip_filtered_attributes(self, mock_model, mock_get_import_definition):
        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
            },
            'gob_mapping': {
                'a': {
                    'filters': ['filter1', 'filter2']
                },
                'b': {
                    'filters': {
                        'sub': ['filter3'],
                        'sub 1': []
                    }
                },
                'c': {
                    'filters': []
                }
            }
        }

        mock_attributes = {
            'a': {
                'type': 'GOB.String'
            },
            'b': {
                'type': 'GOB.String'
            },
            'c': {
                'type': 'GOB.String'
            },
            'd': {
                'type': 'GOB.String'
            }
        }
        mock_model.return_value.get_collection.return_value = {'attributes': mock_attributes}
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual(instance.ignore_columns, instance.default_ignore_columns + ['a', 'b', 'b_sub'])

mock_get_import_definition = MagicMock()

@patch("gobtest.data_consistency.data_consistency_test.get_import_definition", mock_get_import_definition)
@patch("gobtest.data_consistency.data_consistency_test.GOBModel", MagicMock())
class TestDataConsistencyTest(TestCase):

    def setUp(self) -> None:
        mock_import_definition = {
            'source': {
                'name': 'any name',
                'application': 'any application',
                'entity_id': 'any entity id'
            }
        }
        mock_get_import_definition.return_value = mock_import_definition

    @patch("gobtest.data_consistency.data_consistency_test.ProgressTicker", MagicMock())
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
        inst._get_gob_count = lambda: 13

        inst.run()
        mock_random.randint.assert_called_with(0, 3)
        inst._get_matching_gob_row.assert_has_calls([
            call(0),
            call(2),
            call(6),
            call(10),
        ])
        inst._validate_row.assert_has_calls([
            call(2, 4),
            call(6, 12),
        ])

        mock_logger.warning.assert_called_with('Row with id row id missing')
        mock_logger.error.assert_called_with('Have 2 missing rows in GOB, of 4 total rows.')
        mock_logger.info.assert_called_with('Completed data consistency test on 4 rows of 13 rows total. '
                                            '1 rows contained errors. 2 rows could not be found.')

        # Check count mismatch
        mock_logger.error.reset_mock()
        inst._get_gob_count = lambda: 0
        inst.run()
        mock_logger.error.assert_any_call("Counts don't match: source 13 - GOB 0 (13)")
        mock_logger.error.assert_any_call('Have 2 missing rows in GOB, of 4 total rows.')
        self.assertEqual(mock_logger.error.call_count, 2)

    @patch("gobtest.data_consistency.data_consistency_test.ProgressTicker", MagicMock())
    @patch("gobtest.data_consistency.data_consistency_test.logger")
    @patch("gobtest.data_consistency.data_consistency_test.random")
    def test_run_merged_dataset(self, mock_random, mock_logger):
        mock_random.randint.return_value = 2
        inst = DataConsistencyTest('cat', 'col', 'appl')
        inst.SAMPLE_SIZE = 0.25
        inst._connect = MagicMock()
        inst.has_states = False
        inst._get_row_id = lambda x: 'row id'
        inst.source = {
            'merge': {
                'on': 'id'
            }
        }
        inst.is_merged = True
        inst._get_expected_merge_cnt = MagicMock(return_value=13)
        inst._get_matching_gob_row = MagicMock(side_effect=lambda x: {'id': x['id'] * 2} if x['id'] * 2 % 10 != 0 else None)
        inst._validate_row = MagicMock(side_effect=lambda x, y: x['id'] % 6 == 0)
        inst._get_source_data = MagicMock(return_value=[{'id': x} for x in [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ]])
        inst._get_gob_count = lambda: 13

        inst.run()
        mock_random.randint.assert_called_with(0, 3)
        inst._get_matching_gob_row.assert_has_calls([
            call({'id': 0}),
            call({'id': 2}),
            call({'id': 6}),
            call({'id': 10}),
        ])
        inst._validate_row.assert_has_calls([
            call({'id': 2}, {'id': 4}),
            call({'id': 6}, {'id': 12}),
        ])

        mock_logger.warning.assert_called_with('Row with id row id missing')
        mock_logger.error.assert_called_with('Have 2 missing rows in GOB, of 4 total rows.')
        mock_logger.info.assert_called_with('Completed data consistency test on 4 rows of 13 rows total. '
                                            '1 rows contained errors. 2 rows could not be found.')

        inst._get_expected_merge_cnt.assert_called_with([x for x in range(13)])

    def test_get_expected_merge_cnt_diva_into_dgdialog(self):
        """Tests the case as commented in the tested method

        :return:
        """
        inst = DataConsistencyTest('cat', 'col', 'appl')
        inst.source = {
            'merge': {
                'dataset': 'some dataset',
                'id': 'diva_into_dgdialog',
                'on': 'somefield'
            }
        }

        # Alias is not used. Just a convenience attribute to be able to follow the example in the method comments
        inst._get_merge_data = MagicMock(return_value=[
            {'somefield': 'A', 'alias': 'A1'},
            {'somefield': 'A', 'alias': 'A2'},
            {'somefield': 'B', 'alias': 'B1'},
            {'somefield': 'D', 'alias': 'D1'},
        ])
        merge_ids = ['A', 'A', 'B', 'B', 'C']
        self.assertEqual(7, inst._get_expected_merge_cnt(merge_ids))

        # Test NotImplemented case
        inst.source['merge']['id'] = 'nonexistent'

        with self.assertRaises(NotImplementedError):
            inst._get_expected_merge_cnt(merge_ids)

    def test_geometry_to_wkt(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.analyse_db = MagicMock()
        inst.analyse_db.read.return_value = [['WKT VAL']]

        self.assertEqual('WKT VAL', inst._geometry_to_wkt('geoval'))
        inst.analyse_db.read.assert_called_with("SELECT ST_AsText('geoval'::geometry)")

        self.assertIsNone(inst._geometry_to_wkt(None))

    def test_normalise_wkt(self):
        inst = DataConsistencyTest('cat', 'col')
        test_cases = [
            ('POLYGON ((abcdefg', 'POLYGON((abcdefg'),
            ('POLYGON((12345.688 248024.02400', 'POLYGON((12345 248024'),
            ('POLYGON((11 22, 33 44', 'POLYGON((11 22,33 44'),
            ('POLYGON ((1244.24 8024.22, 248240.22 428025.55))', 'POLYGON((1244 8024,248240 428025))'),
            (None, None)
        ]

        for arg, result in test_cases:
            self.assertEqual(result, inst._normalise_wkt(arg))

    @patch("gobtest.data_consistency.data_consistency_test.get_gob_type_from_info")
    def test_transform_source_row(self, mock_get_gob_type):
        mock_gob_type = MagicMock()
        mock_gob_type.from_value = lambda x, **kwargs: type('Type', (), {'to_value': x})
        mock_get_gob_type.side_effect = lambda attr: attr['return']

        inst = DataConsistencyTest('cat', 'col')
        inst._normalise_wkt = lambda x: 'normalised(' + x + ')'
        inst.collection = {
            'all_fields': {
                'a': {
                    'type': '',
                    'return': GOB.String
                },
                'b': {
                    'type': '',
                    'return': GOB.JSON
                },
                'c': {
                    'type': 'GOB.Reference',
                    'return': GOB.Reference
                },
                'd': {
                    'type': 'GOB.Geo.Geometry',
                    'return': GEO.Geometry
                },
                'e': {
                    'type': '',
                    'return': GOB.String
                },
                'f': {
                    'type': '',
                    'return': GOB.String
                },
                'g': {
                    'type': 'GOB.Reference',
                    'return': GOB.Reference
                },
                'h': {
                    'type': '',
                    'return': GOB.JSON
                },
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
                        'x': 'skip this one'
                    }
                },
                'c': {
                    'source_mapping': {
                        'bronwaarde': '=d',
                    }
                },
                'd': {
                    'source_mapping': 'col d',
                },
                'f': {
                    'source_mapping': 'col f',
                },
                'g': {
                    'source_mapping': {
                        'bronwaarde': 'col g',
                        'other attr': 'should be ignored'
                    }
                },
                'h': {
                    'source_mapping': 'col h',
                },
            }
        }

        source_row = {
            'col a': 'val a',
            'col b1': 'val b1',
            'col b2': 'val b2',
            'col d': 'POINT(1 2)',
            'col g': 'val g',
            'col h': 'this is not a json'
        }

        expected_result = {
            'a': 'val a',
            'b_b1': 'val b1',
            'b_b2': 'val b2',
            'b_x': inst.SKIP_VALUE,
            'c_bronwaarde': 'd',
            'd': 'normalised(POINT (1.000 2.000))',
            'e': inst.SKIP_VALUE,
            'f': inst.SKIP_VALUE,
            'g_bronwaarde': 'val g'
        }

        self.assertEqual(expected_result, inst._transform_source_row(source_row))

    def test_transform_source_value(self):
        inst = DataConsistencyTest('cat', 'col')
        mock_type = MagicMock()
        mock_type.from_value.side_effect = GOBTypeException("any type exception")
        self.assertEqual(inst._transform_source_value(mock_type, "any value", {}), "any value")

    def test_unpack_reference(self):
        inst = DataConsistencyTest('cat', 'col')
        attr_name = 'reference attr'
        mapping = {
            'source_mapping': {
                'bronwaarde': 'source attr'
            }
        }
        source_row = {
            'source attr': 'attr value'
        }
        result = {}

        # Plain reference
        gob_type = Reference
        inst._unpack_reference(gob_type, attr_name, mapping, source_row, result)
        self.assertEqual(result, {'reference attr_bronwaarde': 'attr value'})

        # Plain Many Reference
        gob_type = ManyReference
        source_row = {
            'source attr': ['attr value 1', 'attr value 2']
        }
        inst._unpack_reference(gob_type, attr_name, mapping, source_row, result)
        self.assertEqual(result, {'reference attr_bronwaarde': ['attr value 1', 'attr value 2']})

        # Plain reference to an attribute within a dict
        gob_type = Reference
        mapping['source_mapping']['bronwaarde'] = 'source attr.attr'
        source_row['source attr'] = {'attr': 'sub value', 'other attr': 'other sub value'}
        inst._unpack_reference(gob_type, attr_name, mapping, source_row, result)
        self.assertEqual(result, {'reference attr_bronwaarde': 'sub value'})

        # Many reference to attributes within a list of dicts
        gob_type = ManyReference
        mapping['source_mapping']['bronwaarde'] = 'source attr.attr'
        source_row['source attr'] = [{'attr': 'sub value1', 'other attr': 'other sub value'}, {'attr': 'sub value2'}]
        inst._unpack_reference(gob_type, attr_name, mapping, source_row, result)
        self.assertEqual(result, {'reference attr_bronwaarde': ['sub value1', 'sub value2']})

        # Many reference to attributes within a ';' separated string as list value
        gob_type = ManyReference
        mapping['source_mapping']['bronwaarde'] = 'source attr'
        source_row['source attr'] = 'sub value1;sub value2'
        inst._unpack_reference(gob_type, attr_name, mapping, source_row, result)
        self.assertEqual(result, {'reference attr_bronwaarde': ['sub value1', 'sub value2']})

    def test_equal_values(self):
        inst = DataConsistencyTest('cat', 'col')
        for value in 1, True, 2.5, "any string":
            self.assertTrue(inst.equal_values(value, str(value)))
        self.assertTrue(inst.equal_values([], "[]"))
        self.assertTrue(inst.equal_values([None, None], "[]"))
        self.assertTrue(inst.equal_values([None, None], "[,,]"))
        self.assertTrue(inst.equal_values([1,2], "[1,2]"))
        self.assertTrue(inst.equal_values([1,2,3], "[1, 3, 2]")) # sorted compare
        self.assertFalse(inst.equal_values([1,2], "[1, 2, 3]"))
        self.assertFalse(inst.equal_values([1,2,3], "[1, 2]"))
        self.assertFalse(inst.equal_values([1,2], "[1, False]"))
        self.assertTrue(inst.equal_values([1,False], "[1, False]"))
        self.assertTrue(inst.equal_values([1,2.5], "[1, 2.5]"))
        self.assertTrue(inst.equal_values([0,2.5], "[0, 2.5]"))
        self.assertFalse(inst.equal_values("aap noot mies", "aap noot"))
        self.assertFalse(inst.equal_values("aap noot", "aap noot mies"))
        self.assertTrue(inst.equal_values("  Aap \t nOOt \n", "aap noot"))
        # Date comparison should skip any 00:00:00 time
        self.assertTrue(inst.equal_values("2020-06-20", datetime.date(2020,6,20)))
        self.assertTrue(inst.equal_values("2020-06-20 00:00:00", datetime.date(2020,6,20)))
        # But only for dates
        self.assertTrue(inst.equal_values("2020-06-20 00:00:00", datetime.datetime(2020,6,20,0,0,0)))
        # But not any other time
        self.assertFalse(inst.equal_values("2020-06-20 00:00:01", datetime.date(2020,6,20)))
        self.assertFalse(inst.equal_values("2020-06-20 00:00:01", datetime.datetime(2020,6,20,0,0,2)))

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
        self.assertEqual(inst.gob_key_errors['a'], 'Missing key a in GOB')
        inst._log_result(0, 0, 0, 0, 0)
        mock_logger.error.assert_called_with('Missing key a in GOB')

        # Do not print GOB key errors for attributes that are already logged for the source
        mock_logger.error.reset_mock()
        mock_logger.warning.reset_mock()
        inst.gob_key_errors = {}
        inst.src_key_warnings = {
            'a': 'something wrong with a'
        }
        inst._validate_row(
            {'a': 'aa'},
            {},
        )
        self.assertIsNone(inst.gob_key_errors.get('a'))
        inst._log_result(0, 0, 0, 0, 0)
        mock_logger.error.assert_not_called()
        mock_logger.warning.assert_called_with('something wrong with a')

        mock_logger.error.reset_mock()
        mock_logger.warning.reset_mock()
        inst._validate_row(
            {},
            {'a': 'aa'}
        )
        self.assertEqual(inst.gob_key_errors['a'], 'Have unexpected key left in GOB: a')
        inst._log_result(0, 0, 0, 0, 0)
        mock_logger.error.assert_called_with('Have unexpected key left in GOB: a')

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
                'GOB id': {
                    'source_mapping': 'the id'
                }
            }
        }

        self.assertEqual('a', inst._get_row_id({'the id': 'a'}))

    def test_get_matching_gob_row(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.has_states = True
        inst.analyse_db = MagicMock()
        inst.analyse_db.read.return_value = [{'the': 'row'}]
        inst.entity_id_field = 'ai die'

        inst.import_definition = {
            'source': {
              'name': 'any source',
              'application': 'any application',
              'entity_id': 'idfield'
            },
            'gob_mapping': {
                'GOB id': {
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
        inst.analyse_db.read.assert_called_with("""\
SELECT
    *
FROM
    cat.col
WHERE
    _source = 'any source' AND
    _application = 'any application' AND
    _date_deleted IS NULL AND
    volgnummer = 'SEQNR' AND
    _source_id = 'ID.SEQNR'
""")

        inst.is_merged = True
        # If the dataset is merged with another dataset the sequence number is not guaranteed to match
        # Instead the last known entity is retrieved, independent of the sequence number
        inst._get_matching_gob_row(source_row)
        inst.analyse_db.read.assert_called_with("""\
SELECT
    *
FROM
    cat.col
WHERE
    _source = 'any source' AND
    _application = 'any application' AND
    _date_deleted IS NULL AND
    _source_id LIKE 'ID.%'
""")

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
    @patch("gobtest.data_consistency.data_consistency_test.get_import_definition_by_filename")
    def test_get_merge_data(self, mock_get_import_definition, mock_factory):
        mock_get_import_definition.return_value = {
            'source': {
                'application': 'APPLICATION',
                'read_config': 'THE READ CONFIG',
                'query': ['SOME QUERY']
            },
        }

        inst = DataConsistencyTest('cat', 'col')
        inst.source = {
            'merge': {
                'dataset': 'somedatasetfile.json'
            }
        }

        result = inst._get_merge_data()
        mock_factory.get_datastore.assert_called_with('APPLICATION_CONFIG', 'THE READ CONFIG')
        mock_factory.get_datastore().query.assert_called_with('SOME QUERY')
        mock_factory.get_datastore().connect.assert_called_once()
        self.assertEqual(mock_factory.get_datastore().query(), result)

    def test_gob_count(self):
        inst = DataConsistencyTest('cat', 'col')
        inst._read_from_analyse_db = lambda query: [{'count': 123}]
        self.assertEqual(inst._get_gob_count(), 123)

    def test_read_from_analyse_db(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.analyse_db = MagicMock()
        inst.analyse_db.read.side_effect = lambda query: "any result"
        self.assertEqual(inst._read_from_analyse_db("any query"), "any result")
        inst.analyse_db.read.side_effect = GOBException("any GOB exception")
        self.assertEqual(inst._read_from_analyse_db("any error query"), None)

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
