import json
import random
import re
import operator
import datetime
from functools import reduce
from typing import Iterator, Optional

from gobcore.utils import ProgressTicker
from gobcore.exceptions import GOBException
from gobconfig.import_.import_config import get_import_definition, get_import_definition_by_filename
from gobcore.datastore.factory import DatastoreFactory
from gobconfig.datastore.config import get_datastore_config
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.typesystem import get_gob_type_from_info
from gobcore.typesystem.gob_types import Reference, JSON, IncompleteDate
from gobcore.typesystem.gob_secure_types import Secure
from gobcore.typesystem.gob_geotypes import GEOType
from gobcore.exceptions import GOBTypeException
from gobcore.logging.logger import logger

GOB_DB = 'GOBDatabase'


class NotImplementedCatalogError(GOBException):
    pass


class NotImplementedApplicationError(GOBException):
    pass


def escape(value):
    """Properly escape values in string query."""
    return value.replace("'", "''").replace("%", "%%") if isinstance(value, str) else value


class DataConsistencyTest:
    # How many rows of total rows to check
    SAMPLE_SIZE = 0.001

    BATCH_SIZE = 8_000

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
        FIELD.TID,
    ]

    # If no mapping exist for a value, the mapping might be implicit
    # Example: eind_geldigheid for BRK gemeentes, this field is not present in the source
    # If no source value exist from the import query it might be an enriched value
    # Example: code and gemeentenaam in row properties of BRK gemeentes
    SKIP_VALUE = "### SKIP VALUE ###"

    def __init__(self, catalog_name: str, collection_name: str, application: str = None):

        if catalog_name == 'rel':
            raise NotImplementedCatalogError("Not implemented for the 'rel' catalog")

        if application == 'BAGExtract':
            raise NotImplementedApplicationError("Not implemented for BAGExtract")

        self.import_definition = get_import_definition(catalog_name, collection_name, application)
        self.source = self.import_definition['source']
        self.catalog_name = catalog_name
        self.collection_name = collection_name
        self.application = application
        self.collection = GOBModel().get_collection(catalog_name, collection_name)
        self.entity_id_field = self.source['entity_id']
        self.has_states = self.collection.get('has_states', False)
        self.gob_key_errors = {}
        self.src_key_warnings = {}
        self.is_merged = self.source.get('merge') is not None
        self.compared_columns = []

        # Ignore enriched attributes by default
        self.ignore_columns = self.default_ignore_columns + list(self.source.get('enrich', {}).keys()) + \
            self.import_definition.get('not_provided_attributes', [])

        # If the dataset is merged with another dataset the seqnr might be altered
        if self.is_merged:
            self.ignore_columns.append(FIELD.SEQNR)

        # Ignore secure columns
        self.ignore_secure_columns()

        # Ignore columns that have modified values
        self.ignore_filtered_columns()

        # Ignore columns that have enriched values in GOB-Import
        self.ignore_enriched_columns()

    def ignore_filtered_columns(self):
        """
        Ignore any fields that have a filter definition

        A filter definition is a non-empty array of filters that are applied to any field value

        Filtered fields have values do not correspond 1-1 to a source value
        and are therefor skipped in the comparison
        :return:
        """
        for attribute, type_info in self.collection['attributes'].items():
            mapping = self.import_definition['gob_mapping'].get(attribute)
            if mapping and mapping.get('filters'):
                # Ignore columns whose values are modified (eg to uppercase, ...)
                self.ignore_columns.append(attribute)
                if isinstance(mapping['filters'], dict):
                    for key in mapping['filters'].keys():
                        if mapping['filters'][key]:
                            self.ignore_columns.append(f"{attribute}_{key}")

    def ignore_secure_columns(self):
        """
        Ignore any secure fields

        Ignore secure columns to prevent leakage of private data

        :return:
        """
        for attribute, type_info in self.collection['attributes'].items():
            gob_type = get_gob_type_from_info(type_info)
            if issubclass(gob_type, Secure) or (issubclass(gob_type, Reference) and 'secure' in type_info):
                # Plain secure type or secure reference
                self.ignore_columns.append(attribute)
                if issubclass(gob_type, Reference):
                    self.ignore_columns.append(f"{attribute}_bronwaarde")

    def ignore_enriched_columns(self):
        """
        Ignore any enriched fields

        :return:
        """
        for attribute, type_info in self.collection['attributes'].items():
            mapping = self.import_definition['gob_mapping'].get(attribute)
            if mapping and mapping.get('enriched'):
                # Ignore columns whose values are enriched (eg to split to JSON Arrays, ...)
                if type_info.get('attributes'):
                    # For JSON fields with attributes defined ignore the specific attributes
                    for key in type_info.get('attributes').keys():
                        self.ignore_columns.append(f"{attribute}_{key}")
                else:
                    self.ignore_columns.append(attribute)

    def run(self):
        self._connect()
        test_every = 1 / self.SAMPLE_SIZE
        random_offset = random.randint(0, int(test_every - 1))

        rows = self._get_source_data()
        cnt = 0
        checked = 0
        success = 0
        missing = 0
        merge_ids = []
        merge_id = self.source.get('merge', {}).get('on')

        gob_count = self._get_gob_count()
        logger.info(f"Aantal {self.catalog_name} {self.collection_name} in GOB: {gob_count:,}")

        with ProgressTicker(f"Compare data ({gob_count:,})", 10000) as progress:
            for row in rows:
                progress.tick()

                # Always test the first row, test other rows at random
                if cnt == 0 or cnt % test_every == random_offset:
                    gob_rows = self._get_matching_gob_rows(row)

                    if not gob_rows:
                        seqnr = f' and volgnummer {row.get(FIELD.SEQNR)}' if self.has_states else ''
                        logger.warning(f"Row with id {self._get_row_id(row)}{seqnr} missing")
                        missing += 1
                    else:
                        success += int(self._validate_minimal_one_row(row, gob_rows))

                    checked += 1

                cnt += 1

                if merge_id:
                    merge_ids.append(row[merge_id])

        if self.is_merged:
            cnt = self._get_expected_merge_cnt(merge_ids)

        logger.info(f"Aantal {self.catalog_name} {self.collection_name} in source: {cnt:,}")
        logger.info(f"Ignored columns: {', '.join(self.ignore_columns)}")
        logger.info(f"Compared columns: {', '.join(self.compared_columns)}")

        self._log_result(checked, cnt, gob_count, missing, success)

    def _get_expected_merge_cnt(self, merge_ids: list):
        merge_def = self.source.get('merge')

        merge_objects = self._get_merge_data()

        if merge_def.get('id') == 'diva_into_dgdialog':
            """The diva_into_dgdialog merge method merges the last states from DIVA into the first state of DGDialog.
            DGDialog is the main source, DIVA is the merged source.

            This means that we're expecting to have:
            1. ALL DGDialog objects and states, plus
            2. All states from the merged collection, minus the states that were merged with DGDialog states.

            Regarding 2, this means that for all objects in the merged data:
            - If the object is present in DGDialog, we add number_of_states_DIVA(object) - 1, because the last state
              was merged.
            - If the object is not present in DGDialog, we add number_of_states_DIVA(object)

            If DGDialog has objects (with states) A1, A2, B1, B2, C1
            And DIVA has A1, A2, B1, D1

            A2(DIVA) will be merged with A1(DGDialog), so we have A1(DIVA), A1(DGDialog), A2(DGDialog)
            B1(DIVA) will be merged with B1(DGDialog), so we have B1(DGDialog)
            C1 only exists in DGDialog, so we have C1(DGDialog)
            D1 only exists in DIVA, so we have D1(DIVA)

            We end up with a count of:
            - All objects and states in DGDialog: A1, A2, B1, B2 and C1 totals 5
            - Plus number_of_states_DIVA(object) - 1 for all objects that match DGDialog. This is the case for objects
              A and B (2 - 1) + (1 - 1) = 1
            - Plus number_of_states_DIVA(object) for all objects that don't match DGDialog. This is the case for object
              D, so we add a count of 1.

            The expected number of rows in GOB is thus 5 + 1 + 1 = 7.

            The example described here is also implemented as a test.
            """
            on = merge_def.get('on')

            # Collect id's with counts from merged data, where id is the field that is used to match the two sources
            ids = reduce(lambda x, y: x.update({y[on]: x.get(y[on], 0) + 1}) or x, merge_objects, {})

            expected_cnt = len(merge_ids) + sum([cnt if id_ not in merge_ids else cnt - 1 for id_, cnt in ids.items()])

            return expected_cnt
        else:
            raise NotImplementedError(f"Merge id {merge_def.get('id')} not implemented")

    def _log_result(self, checked, cnt, gob_count, missing, success):
        if gob_count != cnt:
            logger.error(f"Counts don't match: source {cnt:,} - GOB {gob_count:,} ({abs(cnt - gob_count):,})")

        if missing > 0:
            logger.error(f"Have {missing:,} missing rows in GOB, of {checked:,} total rows.")

        for key_error in self.gob_key_errors.values():
            logger.error(key_error)

        for key_warning in self.src_key_warnings.values():
            logger.warning(key_warning)

        logger.info(f"Completed data consistency test on {checked:,} rows of {cnt:,} rows total." +
                    f" {(checked - success - missing):,} rows contained errors." +
                    f" {missing:,} rows could not be found.")

    def _src_key_warning(self, attr_name, msg):
        self.src_key_warnings[attr_name] = msg

    def _gob_key_error(self, attr_name, msg):
        if attr_name not in self.src_key_warnings:
            # Don't report about something already noticed in the source
            self.gob_key_errors[attr_name] = msg

    def _geometry_to_wkt(self, geo_value: str):
        if geo_value is None:
            return None
        result = next(self._read_from_gob_db(f"SELECT ST_AsText('{geo_value}'::geometry)"))
        return result[0] if result else None

    @staticmethod
    def _normalise_wkt(wkt_value: str):
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

    def _transform_source_value(self, type_, value, mapping):
        """
        Transform the source value so that it can be compared with the GOB value

        :param type_:
        :param value:
        :param mapping:
        :return:
        """
        try:
            # Let GOB typesystem handle formatting
            value = type_.from_value(value, **mapping).to_value
        except GOBTypeException:
            # Stick with the raw source value
            pass
        else:
            # No exception
            if issubclass(type_, GEOType):
                value = self._normalise_wkt(value)
        return value

    def _transform_source_row(self, source_row: dict):
        """Transforms rows from source database to the format the row should appear in the gob database, based on
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
                self._src_key_warning(attr_name, f"Skipped {attr_name} because no mapping is found")
            else:
                source_mapping = mapping['source_mapping']
                type_ = get_gob_type_from_info(attr)

                if issubclass(type_, JSON):
                    self._unpack(type_, attr_name, mapping, source_row, result)
                    continue
                elif source_mapping and source_mapping[0] == '=':
                    value = self._transform_source_value(type_, source_mapping[1:], mapping)
                elif source_mapping in source_row:
                    value = self._transform_source_value(type_, source_row[source_mapping], mapping)
                else:
                    self._src_key_warning(attr_name, f"Skipped {attr_name} because it is missing in the input")
                    value = self.SKIP_VALUE

            result[attr_name] = value

        return result

    def _unpack(self, type_, attr_name: str, mapping: dict, source_row: dict, result: dict):
        args = (attr_name, mapping, source_row, result)
        if issubclass(type_, Reference):
            # Reference, ManyReference and VeryManyReference
            self._unpack_reference(type_, *args)
        elif issubclass(type_, IncompleteDate):
            self._unpack_incomplete_date(*args)
        else:
            self._unpack_json(*args)

    def _unpack_incomplete_date(self, attr_name, mapping, source_row, result):
        # Only unpack available attributes from model
        # source_mapping should be a str referring to the source column with json data
        source_mapping = mapping['source_mapping']
        model_attr = self.collection['all_fields'].get(attr_name)

        for nested_gob_key in model_attr['attributes']:
            value = source_row.get(source_mapping, self.SKIP_VALUE)

            if value != self.SKIP_VALUE:
                value = IncompleteDate(value).to_value[nested_gob_key]

            result[f'{attr_name}_{nested_gob_key}'] = value

    @staticmethod
    def _unpack_string_list(string: str) -> list[str]:
        """
        Unpack a string as a list

        Take the separator as being the character that occurs the most in the given string
        from a list a possible separators

        :param string:
        :return:
        """
        # Count the number of occurences for each possible separator
        counts = {sep: string.count(sep) for sep in [';', ',', ':']}
        # Take the separator with the highest count
        separator = max(counts.items(), key=operator.itemgetter(1))[0]
        # Return a list from the string splitted on the separator and each value trimmed
        return [v.strip() for v in string.split(separator) if v]

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
            if value and not isinstance(value, list):
                # Unpack the value as a string that represents an array
                value = self._unpack_string_list(str(value))
            dst_value = [item[attr] if attr else item for item in value] if value else []

        result[dst_key] = dst_value

    def _format(self, format_def: dict, value):
        if 'split' in format_def:
            return [] if value is None else value.split(format_def['split'])

        raise NotImplementedError("Format action not implemented")

    def _unpack_json(self, attr_name, mapping, source_row, result):
        """
        Unpack a JSON GOB field (not being a Reference, this is handled in _unpack_reference)

        :param attr_name:
        :param mapping:
        :param source_row:
        :return:
        """
        FORMAT = "format"
        source_mapping = mapping['source_mapping']

        model_attr = self.collection['all_fields'].get(attr_name)

        if isinstance(source_mapping, dict):
            for nested_gob_key, source_key in source_mapping.items():
                if nested_gob_key == FORMAT:
                    continue
                # Skip values that are not found, e.g. BAG verblijfsobjecten fng_omschrijving that is set in code
                dst_key = f'{attr_name}_{nested_gob_key}'
                result[dst_key] = source_row.get(source_key, self.SKIP_VALUE)

                if FORMAT in source_mapping:
                    result[dst_key] = self._format(source_mapping[FORMAT], result[dst_key])

        elif model_attr.get('has_multiple_values'):
            # The source data can sometimes be received as a string (Oracle json_arrayagg returns a string)
            json_source_data = self._load_json_source_data(source_mapping, source_row)

            # For multi value JSON values we need to unpack the items to match the format in the gob database
            for nested_gob_key in model_attr.get('attributes'):
                dst_key = f'{attr_name}_{nested_gob_key}'

                src_result = [obj[nested_gob_key] for obj in json_source_data] if json_source_data else None
                result[dst_key] = src_result
        else:
            # Skip JSON's that are not imported per attribute
            self._src_key_warning(attr_name, f"Skip JSON {attr_name} that is imported as non- or empty-JSON")

    @staticmethod
    def _load_json_source_data(source_mapping: dict, source_row: dict):
        """
        Try to load the json_data since we sometimes receive a string from the source database

        :param source_mapping:
        :param source_row:
        :return:
        """
        try:
            return json.loads(source_row.get(source_mapping))
        except (TypeError, json.decoder.JSONDecodeError):
            return source_row.get(source_mapping)

    def _transform_gob_row(self, gob_row: dict):

        for geo_key in [k for k, v in self.collection['all_fields'].items() if v['type'].startswith('GOB.Geo')]:
            gob_row[geo_key] = self._normalise_wkt(self._geometry_to_wkt(gob_row[geo_key]))

        attributes = {k: v for k, v in self.collection['all_fields'].items() if k not in self.ignore_columns}
        result = {}

        for attr_name, attr in attributes.items():
            mapping = self.import_definition['gob_mapping'].get(attr_name)
            type_ = get_gob_type_from_info(attr)

            if issubclass(type_, JSON):
                for key in mapping['source_mapping'].keys():
                    result[f"{attr_name}_{key}"] = gob_row[attr_name].get(key)
            else:
                result[attr_name] = gob_row.get(attr_name)

        return result

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
            f"{FIELD.APPLICATION} = '{application}'",
            f"{FIELD.DATE_DELETED} IS NULL"
        ] + (where or [])

        where = " AND\n    ".join(where)
        return f"""\
SELECT
    {select}
FROM
    {self.catalog_name}_{self.collection_name}
WHERE
    {where}
"""

    def _get_gob_count(self):
        """
        Return the number of entities in GOB

        :return:
        """
        query = self._select_from_gob_query(select="count(*)")
        result = next(self._read_from_gob_db(query))
        return dict(result)['count']

    @staticmethod
    def equal_values(src_value, gob_value) -> bool:
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
            src_value = ','.join(sorted([str(v).strip() for v in src_value if v is not None]))
            # Rebuild the GOB list from the string, skipping empty values
            gob_value = ','.join(sorted([str(v).strip() for v in gob_value[1:-1].split(',') if v])) \
                if isinstance(gob_value, str) else gob_value
            return src_value == gob_value
        else:
            # Compare the two values as string without whitespace, case-insensitive
            gob_str_value = re.sub(r"\s+", "", str(gob_value)).lower()
            src_str_value = re.sub(r"\s+", "", str(src_value)).lower()
            if type(gob_value) == datetime.date:
                # Remove any trailing zero-time to allow date to datetime comparison
                src_str_value = re.sub(r"00:00:00$", "", src_str_value)
            return gob_str_value == src_str_value

    def _register_compared_columns(self, columns):
        if not self.compared_columns:
            # Register the columns that have been compared
            self.compared_columns = columns

    def _validate_minimal_one_row(self, source_row: dict, gob_rows: list):
        """Returns true if `source_row` equals minimal one of the elements in `gob_rows`."""
        expected_values = self._transform_source_row(source_row)
        self._register_compared_columns(expected_values.keys())
        start_error_cnt = len(logger.get_errors())

        mismatches = []
        for gob_row in gob_rows:
            gob_row = self._transform_gob_row(gob_row)

            # append empty list if match is found
            mismatches.append(self._find_mismatches(expected_values, gob_row))

            # Check if any keys in GOB are unchecked, but ignore keys generated by GOB
            # empty if all fields are unequal (find_mismatches)
            not_checked_gob_keys = [k for k in gob_row.keys() if not any([k.endswith(w) and len(k) > len(w)
                                                                          for w in ['_volgnummer', '_id', '_ref']])]
            for key in not_checked_gob_keys:
                self.gob_key_errors[key] = f"Have unexpected key left in GOB: {key}"

        if all(mismatches):
            for mismatch in mismatches:
                mismatch_str = ', '.join([f'{m[0]}: {m[1]} / {m[2]}' for m in mismatch])
                logger.error(f"Have mismatching values between source row {self._get_row_id(source_row)} " +
                             f"and GOB (attr: source/GOB): {mismatch_str}")

        return start_error_cnt == len(logger.get_errors())

    def _find_mismatches(self, expected_values: dict, gob_row: dict) -> list:
        """
        Finds mismatching values between `expected_values` and `gob_row`.
        Returns empty list when `gob_row` is a match.
        """
        mismatches = []

        for attr, value in expected_values.items():
            try:
                gob_value = gob_row.pop(attr)
            except KeyError:
                self._gob_key_error(attr, f"Missing key {attr} in GOB")
                continue

            if value != self.SKIP_VALUE and not self.equal_values(value, gob_value):
                # Skip implicit mappings and enriched fields, report unequal values
                mismatches.append((attr, value, gob_value))

        return mismatches

    def _get_row_id(self, source_row: dict):
        return source_row.get(self.entity_id_field)

    def _get_matching_gob_rows(self, source_row: dict) -> list:
        """
        A matching row(s) in the analysis database has the following properties:
        - The _source, _application and _source_id should match

        For entities with state the sequence number should also match

        :param source_row:
        :return:
        """
        source_def = self.import_definition['source']
        source_id = escape(source_row[source_def['entity_id']])

        where = []
        # Compare source ids on equality (=)
        is_source_id = "="
        if self.has_states:
            if self.is_merged:
                source_id = f"{source_id}.%"
                # Compare source ids with wildcard comparison for the sequence number
                is_source_id = "LIKE"
            else:
                seq_nr = source_row[self.import_definition['gob_mapping'][FIELD.SEQNR]['source_mapping']]
                # Select matching sequence number
                where.append(f"{FIELD.SEQNR} = '{seq_nr}'")
                # GOB populates the source_id with the sequence number
                source_id = f"{source_id}.{seq_nr}"

        where.append(f"{FIELD.SOURCE_ID} {is_source_id} '{source_id}'")

        query = self._select_from_gob_query(select="*", where=where)
        result = [dict(res) for res in self._read_from_gob_db(query)]

        return result if result else None

    def _get_source_data(self):
        """Get the source data using a server-side cursor."""
        qry = '\n'.join(self.source.get('query', []))
        return self.src_datastore.query(qry, name='test_src_db_cursor', arraysize=self.BATCH_SIZE, withhold=True)

    def _get_merge_data(self):
        """Returns the data from the merge source for this import

        :return:
        """
        assert self.source.get('merge'), "Called _connect_merge_source without merge definition"

        merge_config = get_import_definition_by_filename(self.source['merge']['dataset'])
        merge_source = merge_config['source']

        merge_connection = DatastoreFactory.get_datastore(get_datastore_config(merge_source['application']),
                                                          merge_source.get('read_config', {}))
        merge_connection.connect()
        return merge_connection.query("\n".join(merge_source.get('query', [])))

    def _connect(self):
        datastore_config = self.source.get('application_config') or get_datastore_config(self.source['application'])

        self.src_datastore = DatastoreFactory.get_datastore(datastore_config, self.source.get('read_config', {}))
        self.src_datastore.connect()

        self.gob_db = DatastoreFactory.get_datastore(get_datastore_config(GOB_DB))
        self.gob_db.connect()

    def _read_from_gob_db(self, query) -> Optional[Iterator]:
        """
        Read from the gob db using server-side cursor. Reconnect if any query fails.
        autocommit = True on the connection would also solve the problem
        but this logic is independent from the DatastoreFactory implementation

        :param query:
        :return:
        """
        try:
            return self.gob_db.query(
                query,
                name='test_gob_db_cursor',
                arraysize=self.BATCH_SIZE,
                withhold=True
            )
        except GOBException as e:
            print("Query failed", str(e), query)
            # If autocommit = False the connection will be blocked for further queries
            # Reconnect explicitly to prevent subsequent SQL errors
            self.gob_db.connect()
            return None
