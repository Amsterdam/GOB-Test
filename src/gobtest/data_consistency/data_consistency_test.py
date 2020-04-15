import random
import re

from gobcore.utils import ProgressTicker
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

    # If no mapping exist for a value, the mapping might be implicit
    # Example: eind_geldigheid for BRK gemeentes, this field is not present in the source
    # If no source value exist from the import query it might be an enriched value
    # Example: code and gemeentenaam in row properties of BRK gemeentes
    SKIP_VALUE = "### SKIP VALUE ###"

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

        # Ignore reference fields, this data is stored in relation tables
        for attr in self.collection['references']:
            self.ignore_columns += [attr, f"{attr}_bronwaarde"]

    def run(self):
        self._connect()
        test_every = 1 / self.SAMPLE_SIZE
        random_offset = random.randint(0, test_every - 1)

        rows = self._get_source_data()
        cnt = 0
        checked = 0
        success = 0
        missing = 0

        gob_count = self._get_gob_count()

        with ProgressTicker(f"Compare data ({gob_count:,})", 10000) as progress:
            for row in rows:
                progress.tick()

                # Always test the first row, test other rows at random
                if cnt == 0 or cnt % test_every == random_offset:
                    gob_row = self._get_matching_gob_row(row)

                    if not gob_row:
                        seqnr = f' and volgnummer {row.get(FIELD.SEQNR)}' if self.has_states else ''
                        logger.warning(f"Row with id {self._get_row_id(row)}{seqnr} missing")
                        missing += 1
                    else:
                        success += int(self._validate_row(row, gob_row))
                    checked += 1

                cnt += 1

                if cnt > 25000:
                    break

        if gob_count != cnt:
            logger.error(f"Counts don't match: source {cnt:,} - GOB {gob_count:,} ({abs(cnt - gob_count)})")

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
            mapping = self.import_definition['gob_mapping'].get(attr_name)

            if mapping is None:
                value = self.SKIP_VALUE
            else:
                source_mapping = mapping['source_mapping']
                if isinstance(source_mapping, dict):
                    self._unpack_json(attr_name, mapping, source_row, result)
                    continue
                elif source_mapping in source_row:
                    # Let GOB typesystem handle formatting
                    type = get_gob_type_from_info(attr)
                    value = type.from_value(source_row[source_mapping], **mapping).to_value

                    if attr['type'].startswith('GOB.Geo'):
                        value = self._normalise_wkt(value)
                else:
                    value = self.SKIP_VALUE

            result[attr_name] = value

        return result

    def _unpack_json(self, attr_name, mapping, source_row, result):
        """
        Unpack a JSON-like GOB field

        This can be a JSON field or a reference
        :param attr_name:
        :param mapping:
        :param source_row:
        :return:
        """
        for nested_gob_key, source_key in mapping['source_mapping'].items():
            dst_key = f'{attr_name}_{nested_gob_key}'

            if source_key[0] == '=':
                # Example "=geometry"
                dst_value = source_key[1:]
            elif nested_gob_key == 'bronwaarde' and "." in source_key:
                # Example "tng_ids.nrn_tng_id"
                key, attr = source_key.split(".")
                value = source_row[key]
                if isinstance(value, list):
                    # Example: Many Reference
                    dst_value = [item[attr] for item in value]
                elif isinstance(value, dict):
                    # Example: Reference
                    dst_value = value[attr]
                else:
                    dst_value = value
            else:
                dst_value = source_row[source_key]

            result[dst_key] = dst_value

    def _transform_gob_row(self, gob_row: dict):

        for geo_key in [k for k, v in self.collection['all_fields'].items() if v['type'].startswith('GOB.Geo')]:
            gob_row[geo_key] = self._normalise_wkt(self._geometry_to_wkt(gob_row[geo_key]))

        return {k: v for k, v in gob_row.items() if k not in self.ignore_columns}

    def _select_from_gob_query(self, select, where=None):
        """
        The GOB data that corresponds with the source is characterised by at least a
        matching source and application

        :return:
        """
        source_def = self.import_definition['source']
        source = source_def['name']
        application = source_def['application']

        where = [
            f"{FIELD.SOURCE} = '{source}'",
            f"{FIELD.APPLICATION} = '{application}'"
        ] + (where or [])
        where = " AND\n    ".join(where)
        return f"""\
SELECT
    {select}
FROM
    {self.catalog_name}.{self.collection_name}
WHERE
    {where}
"""

    def _get_gob_count(self):
        """
        Return the number of entities in GOB

        :return:
        """
        query = self._select_from_gob_query(select="count(*)")
        result = self.analyse_db.read(query)
        return dict(result[0])['count']

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

            if value != self.SKIP_VALUE and str(value) != str(gob_value):
                # Skip implicit mappings and enriched fields, report unequal values
                mismatches.append((attr, value, gob_value))

        # Check if any keys in GOB are unchecked, but ignore keys generated by GOB
        not_checked_gob_keys = [k for k in gob_row.keys() if not any([k.endswith(w) and len(k) > len(w)
                                                                      for w in ['_volgnummer', '_id', '_ref']])]

        if mismatches:
            mismatches_str = ', '.join([f'{m[0]}: {m[1]} / {m[2]}' for m in mismatches])
            logger.error(f"Have mismatching values between source row {self._get_row_id(source_row)} " +
                         f"and GOB (attr: source/GOB): {mismatches_str}")

        if not_checked_gob_keys:
            logger.error(f"Have unexpected keys left in GOB: {','.join(not_checked_gob_keys)}")

        return start_error_cnt == len(logger.get_errors())

    def _get_row_id(self, source_row: dict):
        return source_row.get(self.entity_id_field)

    def _get_matching_gob_row(self, source_row: dict):
        """
        A matching row in the analysis database has the following properties:
        - The _source, _application and _source_id should match

        For entities with state the sequence number should also match

        :param source_row:
        :return:
        """
        source_def = self.import_definition['source']
        source_id = source_row[source_def['entity_id']]

        where = []
        if self.has_states:
            seq_nr = source_row[self.import_definition['gob_mapping'][FIELD.SEQNR]['source_mapping']]
            # Select matching sequence number
            where.append(f"{FIELD.SEQNR} = '{seq_nr}'")
            # GOB populates the source_id with the sequence number
            source_id = f"{source_id}.{seq_nr}"

        # select matching source id
        where.append(f"{FIELD.SOURCE_ID} = '{source_id}'")

        query = self._select_from_gob_query(select="*", where=where)
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
