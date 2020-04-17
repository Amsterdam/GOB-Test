import random
import re

from gobcore.utils import ProgressTicker
from gobcore.exceptions import GOBException
from gobconfig.import_.import_config import get_import_definition
from gobcore.datastore.factory import DatastoreFactory
from gobconfig.datastore.config import get_datastore_config
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.typesystem import get_gob_type_from_info, is_gob_reference_type
from gobcore.typesystem.gob_types import Reference
from gobcore.typesystem.gob_secure_types import Secure
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
        self.gob_key_errors = {}

        # Ignore enriched attributes by default
        self.ignore_columns = self.default_ignore_columns + list(self.source.get('enrich', {}).keys())

        # Ignore secure columns to prevent leakage of private data
        for attribute, type_info in self.collection['attributes'].items():
            gob_type = get_gob_type_from_info(type_info)
            if issubclass(gob_type, Secure) or (issubclass(gob_type, Reference) and 'secure' in type_info):
                # Plain secure type or secure reference
                self.ignore_columns.append(attribute)
                if issubclass(gob_type, Reference):
                    self.ignore_columns.append(f"{attribute}_bronwaarde")

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

        self._log_result(checked, cnt, gob_count, missing, success)

    def _log_result(self, checked, cnt, gob_count, missing, success):
        if gob_count != cnt:
            logger.error(f"Counts don't match: source {cnt:,} - GOB {gob_count:,} ({abs(cnt - gob_count)})")

        if checked and float(missing) / checked > self.MISSING_THRESHOLD:
            logger.error(f"Have {missing} missing rows in GOB, of {checked} total rows.")

        for key_error in self.gob_key_errors.values():
            logger.error(key_error)

        logger.info(f"Completed data consistency test on {checked} rows of {cnt} rows total." +
                    f" {checked - success - missing} rows contained errors." +
                    f" {missing} rows could not be found.")

    def _geometry_to_wkt(self, geo_value: str):
        if geo_value is None:
            return None
        result = self._read_from_analyse_db(f"SELECT ST_AsText('{geo_value}'::geometry)")
        return result[0][0] if result else None

    def _normalise_wkt(self, wkt_value: str):
        """Removes space after type keyword and before first parenthesis in WKT definition, removes spaces after
        comma's and remove decimals to prevent rounding issues.

        POLYGON ((xxxxxx becomes POLYGON((xxxxxx

        :param wkt_value:
        :return:
        """
        if wkt_value is None:
            return None
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
                type = get_gob_type_from_info(attr)
                if is_gob_reference_type(attr['type']):
                    self._unpack_reference(type, attr_name, mapping, source_row, result)
                    continue
                elif isinstance(source_mapping, dict):
                    self._unpack_json(attr_name, mapping, source_row, result)
                    continue
                elif source_mapping in source_row:
                    # Let GOB typesystem handle formatting
                    value = type.from_value(source_row[source_mapping], **mapping).to_value

                    if attr['type'].startswith('GOB.Geo'):
                        value = self._normalise_wkt(value)
                else:
                    value = self.SKIP_VALUE

            result[attr_name] = value

        return result

    def _unpack_reference(self, gob_type, attr_name, mapping, source_row, result):
        """
        Unpack a GOB.Reference

        Handle single and many references, also support dict attribute references

        :param gob_type:
        :param attr_name:
        :param mapping:
        :param source_row:
        :param result:
        :return:
        """
        dst_key = f'{attr_name}_bronwaarde'
        source_key = mapping['source_mapping']['bronwaarde']
        attr = None

        if "." in source_key:
            # Example "tng_ids.nrn_tng_id"
            key, attr = source_key.split(".")
            value = source_row[key]
        elif source_key[0] == '=':
            # Example "=geometry"
            value = source_key[1:]
        else:
            # Regular simple bronwaarde, eg "code"
            value = source_row[source_key]

        if gob_type == Reference:
            # single reference value
            dst_value = value[attr] if attr else value
        else:
            # many reference (list) value
            dst_value = [item[attr] if attr else item for item in value] if value else []

        result[dst_key] = dst_value

    def _unpack_json(self, attr_name, mapping, source_row, result):
        """
        Unpack a JSON GOB field (not being a Reference, this is handled in _unpack_reference)

        :param attr_name:
        :param mapping:
        :param source_row:
        :return:
        """
        for nested_gob_key, source_key in mapping['source_mapping'].items():
            dst_key = f'{attr_name}_{nested_gob_key}'
            result[dst_key] = source_row[source_key]

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
        result = self._read_from_analyse_db(query)
        return dict(result[0])['count']

    def equal_values(self, src_value, gob_value):
        """
        Value equality between source and GOB is normally a simple string comparison

        If however the src_value is an array it has to be compared against a GOB string
        The empty values are eliminated from both the source string as the GOB string
        Finally the regular string comparison can be used
        :param src_value:
        :param gob_value:
        :return:
        """
        if isinstance(src_value, list):
            # Skip any None values from the source list
            src_value = ','.join([str(v).strip() for v in src_value if v is not None])
            # Rebuild the GOB list from the string, skipping empty values
            gob_value = ','.join([str(v).strip() for v in gob_value[1:-1].split(',') if v])
        return str(src_value) == str(gob_value)

    def _validate_row(self, source_row: dict, gob_row: dict) -> bool:
        expected_values = self._transform_source_row(source_row)
        gob_row = self._transform_gob_row(gob_row)

        start_error_cnt = len(logger.get_errors())

        mismatches = []
        for attr, value in expected_values.items():
            try:
                gob_value = gob_row.pop(attr)
            except KeyError:
                self.gob_key_errors[attr] = f"Missing key {attr} in GOB"
                continue

            if value != self.SKIP_VALUE and not self.equal_values(value, gob_value):
                # Skip implicit mappings and enriched fields, report unequal values
                mismatches.append((attr, value, gob_value))

        # Check if any keys in GOB are unchecked, but ignore keys generated by GOB
        not_checked_gob_keys = [k for k in gob_row.keys() if not any([k.endswith(w) and len(k) > len(w)
                                                                      for w in ['_volgnummer', '_id', '_ref']])]

        if mismatches:
            mismatches_str = ', '.join([f'{m[0]}: {m[1]} / {m[2]}' for m in mismatches])
            logger.error(f"Have mismatching values between source row {self._get_row_id(source_row)} " +
                         f"and GOB (attr: source/GOB): {mismatches_str}")

        for key in not_checked_gob_keys:
            self.gob_key_errors[key] = f"Have unexpected key left in GOB: {key}"

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

    def _read_from_analyse_db(self, query):
        """
        Read from the analyse db. Reconnect if any query fails.
        autocommit = True on the connection would also solve the problem
        but this logic is independent from the DatastoreFactory implementation

        :param query:
        :return:
        """
        try:
            return self.analyse_db.read(query)
        except GOBException as e:
            print("Query failed", str(e), query)
            # If autocommit = False the connection will be blocked for further queries
            # Reconnect explicitly to prevent subsequent SQL errors
            self.analyse_db.connect()
            return None
