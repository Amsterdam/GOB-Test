from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import datetime

from gobcore.typesystem.gob_types import ManyReference
from gobcore.typesystem import GOB, GEO

from gobtest.data_consistency.data_consistency_test import DataConsistencyTest, GOBException
from gobtest.data_consistency.data_consistency_test import GOBTypeException, Reference, FIELD
from gobtest.data_consistency.data_consistency_test import NotImplementedCatalogError, NotImplementedApplicationError

from gobtest import gob_model


@patch("gobtest.data_consistency.data_consistency_test.get_import_definition")
@patch("gobtest.data_consistency.data_consistency_test.gob_model", spec_set=True)
class TestDataConsistencyTestInit(TestCase):
    """Tests only constructor."""

    def test_init(self, mock_model, mock_get_import_definition):
        mock_gobmodel_data = {
            'the cat': {
                'collections': {
                    'the col': {
                        'has_states': 'SioNo',
                        'references': [],
                        'attributes': {}
                    }
                }
            }
        }
        mock_model.__getitem__.return_value = mock_gobmodel_data['the cat']
        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
                'enrich': {
                    'enrich_column': {},
                }
            },
            'not_provided_attributes': [
                'not_provided_attr_a',
                'not_provided_attr_b',
            ]
        }

        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual('THE ENTITY ID', instance.entity_id_field)
        self.assertEqual('SioNo', instance.has_states)

        self.assertEqual([
            # Default
            'ref',
            '_source',
            '_application',
            '_source_id',
            '_last_event',
            '_hash',
            '_version',
            '_date_created',
            '_date_confirmed',
            '_date_modified',
            '_date_deleted',
            '_gobid',
            '_id',
            '_tid',
            # Enriched
            'enrich_column',
            # From not_provided_attributes
            'not_provided_attr_a',
            'not_provided_attr_b'
        ], instance.ignore_columns)

        self.assertEqual(
            mock_gobmodel_data['the cat']['collections']['the col'], instance.collection)

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

    def test_init_bagextarct(self, mock_model, mock_get_import_definition):
        with self.assertRaises(NotImplementedApplicationError):
            DataConsistencyTest('cat', 'coll', 'BAGExtract')

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
        mock_gobmodel_data = {
            'the cat': {
                'collections': {
                    'the col': {
                        'attributes': mock_attributes
                    }
                }
            }
        }
        mock_model.__getitem__.return_value = mock_gobmodel_data['the cat']
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual(
            instance.ignore_columns, instance.default_ignore_columns + ['a', 'b', 'b_bronwaarde'])

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
        mock_gobmodel_data = {
            'the cat': {
                'collections': {
                    'the col': {
                        'attributes': mock_attributes
                    }
                }
            }
        }
        mock_model.__getitem__.return_value = mock_gobmodel_data['the cat']
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual(
            instance.ignore_columns, instance.default_ignore_columns + ['a', 'b', 'b_sub'])

    def test_init_skip_enriched_attributes(self, mock_model, mock_get_import_definition):
        mock_get_import_definition.return_value = {
            'source': {
                'entity_id': 'THE ENTITY ID',
            },
            'gob_mapping': {
                'a': {
                    'enriched': True
                },
                'b': {
                    'enriched': True
                },
                'c': {
                    'enriched': False
                },
                'd': {}
            }
        }

        mock_attributes = {
            'a': {
                'type': 'GOB.String'
            },
            'b': {
                'type': 'GOB.JSON',
                'attributes': {
                    'sub1': {
                        'type': 'GOB.String'
                    },
                    'sub2': {
                        'type': 'GOB.String'
                    }
                }
            },
            'c': {
                'type': 'GOB.String'
            },
            'd': {
                'type': 'GOB.String'
            }
        }
        mock_gobmodel_data = {
            'the cat': {
                'collections': {
                    'the col': {
                        'attributes': mock_attributes
                    }
                }
            }
        }
        mock_model.__getitem__.return_value = mock_gobmodel_data['the cat']
        instance = DataConsistencyTest('the cat', 'the col', 'the appl')
        self.assertEqual(
            instance.ignore_columns, instance.default_ignore_columns + ['a', 'b_sub1', 'b_sub2'])


mock_get_import_definition = MagicMock()

@patch("gobtest.data_consistency.data_consistency_test.get_import_definition", mock_get_import_definition)
@patch("gobtest.data_consistency.data_consistency_test.gob_model", MagicMock(spec_set=gob_model))
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
        inst._get_matching_gob_rows = MagicMock(side_effect=lambda x: x * 2 if x * 2 % 10 != 0 else None)
        inst._validate_minimal_one_row = MagicMock(side_effect=lambda x, y: x % 6 == 0)
        inst._get_source_data = MagicMock(return_value=[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ])
        inst._get_gob_count = lambda: 13

        inst.run()
        mock_random.randint.assert_called_with(0, 3)
        inst._get_matching_gob_rows.assert_has_calls([
            call(0),
            call(2),
            call(6),
            call(10),
        ])
        inst._validate_minimal_one_row.assert_has_calls([
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
        inst._get_matching_gob_rows = MagicMock(side_effect=lambda x: {'id': x['id'] * 2} if x['id'] * 2 % 10 != 0 else None)
        inst._validate_minimal_one_row = MagicMock(side_effect=lambda x, y: x['id'] % 6 == 0)
        inst._get_source_data = MagicMock(return_value=[{'id': x} for x in [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ]])
        inst._get_gob_count = lambda: 13

        inst.run()
        mock_random.randint.assert_called_with(0, 3)
        inst._get_matching_gob_rows.assert_has_calls([
            call({'id': 0}),
            call({'id': 2}),
            call({'id': 6}),
            call({'id': 10}),
        ])
        inst._validate_minimal_one_row.assert_has_calls([
            call({'id': 2}, {'id': 4}),
            call({'id': 6}, {'id': 12}),
        ])

        mock_logger.warning.assert_called_with('Row with id row id missing')
        mock_logger.error.assert_called_with('Have 2 missing rows in GOB, of 4 total rows.')
        mock_logger.info.assert_called_with('Completed data consistency test on 4 rows of 13 rows total. '
                                            '1 rows contained errors. 2 rows could not be found.')

        inst._get_expected_merge_cnt.assert_called_with(list(range(13)))

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
        inst.gob_db = MagicMock()
        inst.gob_db.query.return_value = iter([['WKT VAL']])
        query_kwargs = {
            'name': 'test_gob_db_cursor',
            'arraysize': inst.BATCH_SIZE,
            'withhold': True
        }

        self.assertEqual('WKT VAL', inst._geometry_to_wkt('geoval'))
        inst.gob_db.query.assert_called_with("SELECT ST_AsText('geoval'::geometry)", **query_kwargs)

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
                'i': {
                    'type': '',
                    'return': GOB.JSON,
                    'has_multiple_values': True,
                    'attributes': {
                        "code": {
                            "type": "GOB.String"
                        },
                        "omschrijving": {
                            "type": "GOB.String"
                        }
                    }
                },
                'j': {
                    'type': '',
                    'return': GOB.JSON,
                    'has_multiple_values': True,
                    'attributes': {
                        "code": {
                            "type": "GOB.String"
                        },
                        "omschrijving": {
                            "type": "GOB.String"
                        }
                    }
                },
                'k': {
                    'type': '',
                    'return': GOB.JSON,
                    'has_multiple_values': True,
                    'attributes': {
                        'code': {
                            'type': 'GOB.String',
                        },
                        'omschrijving': {
                            'type': 'GOB.String',
                        }
                    }
                },
                'l': {
                    'type': '',
                    'return': GOB.JSON,
                    'has_multiple_values': True,
                    'attributes': {
                        'code': {
                            'type': 'GOB.String',
                        },
                        'omschrijving': {
                            'type': 'GOB.String',
                        }
                    }
                },
                'm': {
                    'type': 'GOB.IncompleteDate',
                    'return': GOB.IncompleteDate,
                    'attributes': {
                        'formatted': {
                            'type': 'GOB.String',
                        },
                        'year': {
                            'type': 'GOB.String',
                        }
                    }
                },
                'n': {
                    'type': 'GOB.String',
                    'return': GOB.String,
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
                'i': {
                    'source_mapping': 'col i',
                },
                'j': {
                    'source_mapping': 'col j',
                },
                'k': {
                    'source_mapping': {
                        'omschrijving': 'col k',
                        'format': {
                            'split': ';'
                        }
                    }
                },
                'l': {
                    'source_mapping': {
                        'omschrijving': 'col l',
                        'format': {
                            'split': ';'
                        }
                    }
                },
                'm': {
                    'source_mapping': 'col m'
                },
                'n': {
                    'source_mapping': '=CONSTANT'
                }
            }
        }

        source_row = {
            'col a': 'val a',
            'col b1': 'val b1',
            'col b2': 'val b2',
            'col d': 'POINT(1 2)',
            'col g': 'val g',
            'col h': 'this is not a json',
            'col i': [{"code": "code_1", "omschrijving": "omschrijving_1"}, {"code": "code_2", "omschrijving": "omschrijving_2"}],
            'col j': '[{"code": "code_1", "omschrijving": "omschrijving_1"}, {"code": "code_2", "omschrijving": "omschrijving_2"}]',
            'col k': 'A;B;C',
            'col l': None,
            'col m': '2020-00-00',
            'col n': None
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
            'g_bronwaarde': 'val g',
            'i_code': ['code_1', 'code_2'],
            'i_omschrijving': ['omschrijving_1', 'omschrijving_2'],
            'j_code': ['code_1', 'code_2'],
            'j_omschrijving': ['omschrijving_1', 'omschrijving_2'],
            'k_omschrijving': ['A', 'B', 'C'],
            'l_omschrijving': [],
            'm_formatted': '2020-00-00',
            'm_year': 2020,
            'n': 'CONSTANT'
        }

        self.assertEqual(expected_result, inst._transform_source_row(source_row))

    def test_format_not_implemented(self):
        inst = DataConsistencyTest('cat', 'col')
        with self.assertRaises(NotImplementedError):
            inst._format({}, 'some value')

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
        self.assertTrue(inst.equal_values([], []))
        self.assertTrue(inst.equal_values([1, 2], [2, 1]))
        self.assertTrue(inst.equal_values(["1", 2], ["2", 1]))
        self.assertTrue(inst.equal_values([1, 2, 3], [2, 1, 3]))
        self.assertTrue(inst.equal_values([1, 2], [1, None, 2]))
        self.assertTrue(inst.equal_values([None, 1, 2], [1, None, 2]))
        self.assertTrue(inst.equal_values([None, 1, 2], [1, None, 2, None]))
        self.assertTrue(inst.equal_values([None, None, 1, 2], [1, None, 2]))
        self.assertFalse(inst.equal_values([1], [1, 1]))
        self.assertFalse(inst.equal_values([1, 1], [1]))
        self.assertFalse(inst.equal_values([1, 2], [2, 1, 1]))
        self.assertFalse(inst.equal_values([1, 2], [2, 1, 1]))

        self.assertFalse(inst.equal_values("aap noot mies", "aap noot"))
        self.assertFalse(inst.equal_values("aap noot", "aap noot mies"))
        self.assertTrue(inst.equal_values("  Aap \t nOOt \n", "aap noot"))
        # Date comparison should skip any 00:00:00 time
        self.assertTrue(inst.equal_values("2020-06-20", datetime.date(2020, 6, 20)))
        self.assertTrue(inst.equal_values("2020-06-20 00:00:00", datetime.date(2020, 6, 20)))
        # But only for dates
        self.assertTrue(inst.equal_values("2020-06-20 00:00:00", datetime.datetime(2020, 6, 20, 0, 0, 0)))
        # But not any other time
        self.assertFalse(inst.equal_values("2020-06-20 00:00:01", datetime.date(2020, 6, 20)))
        self.assertFalse(inst.equal_values("2020-06-20 00:00:01", datetime.datetime(2020,6,20,0,0,2)))

    def test_transform_gob_row(self):
        inst = DataConsistencyTest('cat', 'col')
        inst._geometry_to_wkt = lambda x: 'wkt(' + x + ')'
        inst._normalise_wkt = lambda x: 'normalised(' + x + ')'
        inst.import_definition['gob_mapping'] = {
            'geofield': None,
            'jsonfield': {
                'source_mapping': {
                    'a': None,
                    'b': None,
                    'format': None,
                    'begin_geldigheid': None,
                    'eind_geldigheid': None,
                }
            },
            'reffield': {
                'source_mapping': {
                    'bronwaarde': None,
                    'format': None,
                    'begin_geldigheid': None,
                    'eind_geldigheid': None,
                }
            },
            'listjsonfield': {
                'source_mapping': {
                    'a': None,
                    'b': None,
                    'format': None,
                    'begin_geldigheid': None,
                    'eind_geldigheid': None,
                }
            },
            'jsonfield_nodictmapping': {
                'source_mapping': 'someval'
            },
            'jsonfield_withnogobvalue': {
                'source_mapping': {
                    'f': None,
                    'g': None,
                }
            }
        }
        inst.collection = {
            'all_fields': {
                'geofield': {
                    'type': 'GOB.Geo.Point',
                },
                'jsonfield': {
                    'type': 'GOB.JSON',
                },
                'reffield': {
                    'type': 'GOB.Reference',
                },
                'listjsonfield': {
                    'type': 'GOB.JSON',
                },
                'jsonfield_nodictmapping': {
                    'type': 'GOB.JSON',
                    # Because the source mapping is not a dict, the attributes defined in the model (here) are used
                    'attributes': {
                        'c': None,
                    }
                },
                'jsonfield_withnogobvalue': {
                    'type': 'GOB.JSON',
                }
            }
        }
        gob_row = {
            'geofield': 'geovalue',
            '_date_deleted': 'ignored',
            'jsonfield': {
                'a': 'The value for A',
                'b': 'B value'
            },
            'reffield': {
                'bronwaarde': 'the zorz value'
            },
            'listjsonfield': [{'a': 'first A', 'b': 'first B'}, {'a': 'second A', 'b': 'second B'}],
            'jsonfield_nodictmapping': [{'c': 'first C', 'd': 'first D'}, {'c': 'second C', 'd': 'second D'}],
            'jsonfield_withnogobvalue': None,
        }

        self.assertEqual({
            'geofield': 'normalised(wkt(geovalue))',
            'jsonfield_a': 'The value for A',
            'jsonfield_b': 'B value',
            'reffield_bronwaarde': 'the zorz value',
            'listjsonfield_a': ['first A', 'second A'],
            'listjsonfield_b': ['first B', 'second B'],
            'jsonfield_nodictmapping_c': ['first C', 'second C'],
            'jsonfield_withnogobvalue_f': None,
            'jsonfield_withnogobvalue_g': None,
        }, inst._transform_gob_row(gob_row))

    @patch("gobtest.data_consistency.data_consistency_test.logger")
    def test_validate_row(self, mock_logger):

        # First test if errors are logged correctly
        inst = DataConsistencyTest('cat', 'col')
        inst._transform_gob_row = lambda x: x
        inst._transform_source_row = lambda x: x

        inst._validate_minimal_one_row({}, [{}])
        mock_logger.error_assert_not_called()

        inst._validate_minimal_one_row({'a': 'aa'}, [{'a': 'aa'}])
        mock_logger.error_assert_not_called()

        inst._validate_minimal_one_row(
            {'a': 'aa'},
            [{'a': 'ab'}],
        )
        mock_logger.error.assert_called_once()

        mock_logger.error.reset_mock()
        inst._validate_minimal_one_row(
            {'a': 'aa'},
            [{}],
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
        inst._validate_minimal_one_row(
            {'a': 'aa'},
            [{}],
        )
        self.assertIsNone(inst.gob_key_errors.get('a'))
        inst._log_result(0, 0, 0, 0, 0)
        mock_logger.error.assert_not_called()
        mock_logger.warning.assert_called_with('something wrong with a')

        mock_logger.error.reset_mock()
        mock_logger.warning.reset_mock()
        inst._validate_minimal_one_row(
            {},
            [{'a': 'aa'}]
        )
        self.assertEqual(inst.gob_key_errors['a'], 'Have unexpected key left in GOB: a')
        inst._log_result(0, 0, 0, 0, 0)
        mock_logger.error.assert_called_with('Have unexpected key left in GOB: a')

        # Check if the return value is correct based on the error logger

        # First 0 errors, last call contains 1 errors, so no success.
        mock_logger.get_errors.side_effect = [[], [1]]
        self.assertFalse(inst._validate_minimal_one_row({}, [{}]))

        # Errors are empty during both calls, which means no extra errors are logged
        mock_logger.get_errors.side_effect = [[], []]
        self.assertTrue(inst._validate_minimal_one_row({}, [{}]))

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
        inst.gob_db = MagicMock()
        inst.gob_db.query.return_value = iter([{'the': 'row'}])
        inst.entity_id_field = 'ai die'

        query_kwargs = {
            'name': 'test_gob_db_cursor',
            'arraysize': inst.BATCH_SIZE,
            'withhold': True
        }

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
            'idfield': "ID%'",
            'volgnr': 'SEQNR'
        }

        self.assertEqual([{'the': 'row'}], inst._get_matching_gob_rows(source_row))
        inst.gob_db.query.assert_called_with("""\
SELECT
    *
FROM
    cat_col
WHERE
    _source = 'any source' AND
    _application = 'any application' AND
    _date_deleted IS NULL AND
    volgnummer = 'SEQNR' AND
    _source_id = 'ID%%''.SEQNR'
""", **query_kwargs)

        inst.is_merged = True
        # If the dataset is merged with another dataset the sequence number is not guaranteed to match
        # Instead the last known entity is retrieved, independent of the sequence number

        inst._get_matching_gob_rows(source_row)
        inst.gob_db.query.assert_called_with("""\
SELECT
    *
FROM
    cat_col
WHERE
    _source = 'any source' AND
    _application = 'any application' AND
    _date_deleted IS NULL AND
    _source_id LIKE 'ID%%''.%'
""", **query_kwargs)

    def test_get_source_data(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.src_datastore = MagicMock()
        inst.source = {
            'query': ['a', 'b', 'c']
        }
        query_kwargs = {
            'name': 'test_src_db_cursor',
            'arraysize': inst.BATCH_SIZE,
            'withhold': True
        }

        self.assertEqual(inst.src_datastore.query.return_value, inst._get_source_data())
        inst.src_datastore.query.assert_called_with('a\nb\nc', **query_kwargs)

    @patch("gobtest.data_consistency.data_consistency_test.DatastoreFactory")
    @patch("gobtest.data_consistency.data_consistency_test.get_datastore_config", lambda x: x + '_CONFIG')
    @patch("gobtest.data_consistency.data_consistency_test.get_import_definition_by_filename")
    def test_get_merge_data(self, mock_get_import_definition_by_filename, mock_factory):
        mock_get_import_definition_by_filename.return_value = {
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
        inst._read_from_gob_db = lambda query: iter([{'count': 123}])
        self.assertEqual(inst._get_gob_count(), 123)

    def test_read_from_analyse_db(self):
        inst = DataConsistencyTest('cat', 'col')
        inst.gob_db = MagicMock()
        inst.gob_db.query.side_effect = lambda query, name, arraysize, withhold: "any result"
        self.assertEqual(inst._read_from_gob_db("any query"), "any result")
        inst.gob_db.query.side_effect = GOBException("any GOB exception")
        self.assertEqual(inst._read_from_gob_db("any error query"), None)

    @patch("gobtest.data_consistency.data_consistency_test.DatastoreFactory")
    @patch("gobtest.data_consistency.data_consistency_test.get_datastore_config", lambda x: x + '_CONFIG')
    def test_connect(self, mock_factory):
        inst = DataConsistencyTest('cat', 'col')
        inst.source = {'application': 'app'}

        inst._connect()

        mock_factory.get_datastore.assert_has_calls([
            call('app_CONFIG', {}),
            call().connect(),
            call('GOBDatabase_CONFIG'),
            call().connect(),
        ])

    def test_context(self):
        mock_src_ds = MagicMock()
        mock_gob_db = MagicMock()

        with DataConsistencyTest('cat', 'col') as test:
            test.src_datastore = mock_src_ds
            test.gob_db = mock_gob_db

        mock_src_ds.disconnect.assert_called()
        mock_gob_db.disconnect.assert_called()

    def test_context_exception(self):
        mock_src_ds = MagicMock()
        mock_gob_db = MagicMock()

        with self.assertRaises(Exception):
            with DataConsistencyTest('cat', 'col') as test:
                test.src_datastore = mock_src_ds
                test.gob_db = mock_gob_db

                raise Exception

        mock_src_ds.disconnect.assert_called()
        mock_gob_db.disconnect.assert_called()
