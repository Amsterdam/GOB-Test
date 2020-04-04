import random
import re

from gobconfig.import_.import_config import get_import_definition
from gobcore.datastore.factory import DatastoreFactory
from gobconfig.datastore.config import get_datastore_config
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.typesystem import get_gob_type_from_info
from gobcore.logging.logger import logger

ANALYSE_DB = 'GOBAnalyse'


class DataConsistencyTest:
    # How many rows of total rows to check
    SAMPLE_SIZE = 0.001

    # If more than 5% of the requested rows is not present in GOB show error instead of warnings.
    MISSING_THRESHOLD = 0.05

    default_ignore_columns = [
        'ref',
        FIELD.SOURCE,
        FIELD.APPLICATION,
        FIELD.SOURCE_ID,
        FIELD.LAST_EVENT,
        FIELD.HASH,
        FIELD.VERSION,
        FIELD.DATE_CREATED,
        FIELD.DATE_CONFIRMED,
        FIELD.DATE_MODIFIED,
        FIELD.DATE_DELETED,
        FIELD.GOBID,
        FIELD.ID,
    ]

    def __init__(self, catalog_name: str, collection_name: str, application: str = None):
        self.import_definition = get_import_definition(catalog_name, collection_name, application)
        self.source = self.import_definition['source']
        self.catalog_name = catalog_name
        self.collection_name = collection_name
        self.application = application
        self.collection = GOBModel().get_collection(catalog_name, collection_name)
        self.entity_id_field = self.source['entity_id']
        self.has_states = self.collection.get('has_states', False)

        # Ignore enriched attributes by default
        self.ignore_columns = self.default_ignore_columns + list(self.source.get('enrich', {}).keys())

    def run(self):
        self._connect()
        test_every = 1 / self.SAMPLE_SIZE
        random_offset = random.randint(0, test_every - 1)

        rows = self._get_source_data()
        cnt = 0
        checked = 0
        success = 0
        missing = 0

        for row in rows:

            if cnt % test_every == random_offset:
                gob_row = self._get_matching_gob_row(row)

                if not gob_row:
                    seqnr = f' and volgnummer {row.get(FIELD.SEQNR)}' if self.has_states else ''
                    logger.warning(f"Row with id {self._get_row_id(row)}{seqnr} missing")
                    missing += 1
                else:
                    success += int(self._validate_row(row, gob_row))
                checked += 1

            cnt += 1

        if checked and float(missing) / checked > self.MISSING_THRESHOLD:
            logger.error(f"Have {missing} missing rows in GOB, of {checked} total rows.")

        logger.info(f"Completed data consistency test on {checked} rows. {checked - success - missing} rows contained "
                    f"errors. {missing} rows could not be found.")

    def _geometry_to_wkt(self, geo_value: str):
        return self.analyse_db.read(f"SELECT ST_AsText('{geo_value}'::geometry)")[0][0]

    def _normalise_wkt(self, wkt_value: str):
        """Removes space after type keyword and before first parenthesis in WKT definition, removes spaces after
        comma's and remove decimals to prevent rounding issues.

        POLYGON ((xxxxxx becomes POLYGON((xxxxxx

        :param wkt_value:
        :return:
        """
        no_space = re.sub(r'^([A-Z]*) \(', r'\g<1>(', wkt_value)
        comma = re.sub(r', ', ',', no_space)
        fltval = re.sub(r'(\d+)(\.\d+)', r'\g<1>', comma)
        return fltval

    def _transform_source_row(self, source_row: dict):
        """Transforms rows from source database to the format the row should appear in the analyse database, based on
        the GOBModel and import definition mapping.

        :param source_row:
        :return:
        """
        attributes = {k: v for k, v in self.collection['all_fields'].items() if k not in self.ignore_columns}

        result = {}

        for attr_name, attr in attributes.items():
            mapping = self.import_definition['gob_mapping'][attr_name]

            if isinstance(mapping['source_mapping'], dict):
                # JSON-like GOB field. Unpack
                for nested_gob_key, source_key in mapping['source_mapping'].items():
                    dst_key = f'{attr_name}_{nested_gob_key}'

                    if source_key[0] == '=':
                        result[dst_key] = source_key[1:]
                    else:
                        result[dst_key] = source_row[source_key]

                continue

            # Let GOB typesystem handle formatting
            type = get_gob_type_from_info(attr)
            value = type.from_value(source_row.get(mapping['source_mapping']), **mapping).to_value

            if attr['type'].startswith('GOB.Geo'):
                value = self._normalise_wkt(value)
            result[attr_name] = value

        return result

    def _transform_gob_row(self, gob_row: dict):

        for geo_key in [k for k, v in self.collection['all_fields'].items() if v['type'].startswith('GOB.Geo')]:
            gob_row[geo_key] = self._normalise_wkt(self._geometry_to_wkt(gob_row[geo_key]))

        return {k: v for k, v in gob_row.items() if k not in self.ignore_columns}

    def _validate_row(self, source_row: dict, gob_row: dict) -> bool:
        expected_values = self._transform_source_row(source_row)
        gob_row = self._transform_gob_row(gob_row)

        start_error_cnt = len(logger.get_errors())

        mismatches = []
        for attr, value in expected_values.items():
            try:
                gob_value = gob_row.pop(attr)
            except KeyError:
                logger.error(f"Missing key {attr} in GOB")
                continue

            if str(value) != str(gob_value):
                mismatches.append((attr, value, gob_value))

        # Check if any keys in GOB are unchecked, but ignore keys generated by GOB
        not_checked_gob_keys = [k for k in gob_row.keys() if not any([k.endswith(w) and len(k) > len(w)
                                                                      for w in ['_volgnummer', '_id', '_ref']])]

        if mismatches:
            mismatches_str = ', '.join([f'{m[0]}: {m[1]} / {m[2]}' for m in mismatches])
            logger.error(f"Have mismatching values between source and GOB (attr: source/GOB): {mismatches_str}")

        if not_checked_gob_keys:
            logger.error(f"Have unexpected keys left in GOB: {','.join(not_checked_gob_keys)}")

        return start_error_cnt == len(logger.get_errors())

    def _get_row_id(self, source_row: dict):
        return source_row.get(self.import_definition['gob_mapping'][self.entity_id_field]['source_mapping'])

    def _get_matching_gob_row(self, source_row: dict):
        query = f"SELECT * FROM {self.catalog_name}.{self.collection_name} " \
                f"WHERE {FIELD.ID}='{self._get_row_id(source_row)}'"

        if self.has_states:
            query += f" AND {FIELD.SEQNR}=" \
                     f"'{source_row[self.import_definition['gob_mapping'][FIELD.SEQNR]['source_mapping']]}'"

        result = self.analyse_db.read(query)

        return dict(result[0]) if result else None

    def _get_source_data(self):
        return self.src_datastore.query("\n".join(self.source.get('query', [])))

    def _connect(self):
        datastore_config = self.source.get('application_config') or get_datastore_config(self.source['application'])

        self.src_datastore = DatastoreFactory.get_datastore(datastore_config, self.source.get('read_config', {}))
        self.src_datastore.connect()

        self.analyse_db = DatastoreFactory.get_datastore(get_datastore_config(ANALYSE_DB))
        self.analyse_db.connect()
