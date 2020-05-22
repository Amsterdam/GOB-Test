"""Calling this module will run the end-to-end tests.

The end-to-end test tests importing, comparing, storing and relating of entities.

Program exit code is the number of errors that occurred, or -1 in case a system error occurred.

"""

import os
import requests


from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT, END_TO_END_CHECK, RELATE
from gobtest.config import API_HOST


class E2ETest:
    """Class E2ETest

    Tests importing, comparing, updating and relating of entities. Uses GOB-API to verify the correctness of the
    result.

    """
    test_catalog = "test_catalogue"

    test_import_entity = "test_entity"
    test_import_entity_ref = "test_entity_ref"
    test_import_entity_autoid = "test_entity_autoid"
    test_import_entity_autoid_states = "test_entity_autoid_states"
    test_import_entity_reference = "reference"

    # Provide the test_entities in test_import_sources with valid references
    test_import_ref_sources = [
        "ADD",
        "MODIFY1"
    ]

    test_import_autoid_sources = [
        "AUTOID_DELETE",
        "AUTOID",
        "AUTOID_DELETE",
        "AUTOID",
        "AUTOID",
        "AUTOID_ADD",
        "AUTOID_DELETE",
        "AUTOID_MODIFY",
        "AUTOID",
    ]

    test_import_autoid_states_sources = [
        "AUTOID_STATES",
        "AUTOID_STATES_PARTIAL",
        "AUTOID_STATES_PARTIAL_FULL",
    ]

    test_import_sources = [
        "DELETE_ALL",
        "ADD",
        "MODIFY1",
        "DELETE_ALL",
        "ADD",
        "MODIFY1",
    ]

    test_relation_entities = [
        "rel_test_entity_a",
        "rel_test_entity_b",
        "rel_test_entity_c",
        "rel_test_entity_d",
    ]

    test_relation_src_entities = [
        "rel_test_entity_a",
        "rel_test_entity_b",
    ]

    entities_abbreviations = {
        "rel_test_entity_a": "rta",
        "rel_test_entity_b": "rtb",
    }

    test_relation_dst_relations = [
        'rtc_ref_to_c',
        'rtc_manyref_to_c',
        'rtd_ref_to_d',
        'rtd_manyref_to_d'
    ]

    api_base = f"{API_HOST}/gob"

    clear_tests_endpoint = "/alltests/"
    check_import_endpoint = "/test_catalogue/test_entity/?ndjson=true"
    check_autoid_endpoint = "/test_catalogue/test_entity_autoid/?ndjson=true"
    check_autoid_states_endpoint = "/test_catalogue/test_entity_autoid_states/?ndjson=true"

    def _remove_last_event(self, api_response: str):
        """
        Change all last_event values in the API response to an empty string

        :param api_response:
        :return:
        """
        lines = api_response.split('\n')

        # Check header line for last_event column
        firstline = lines[0].split(';')

        try:
            idx = firstline.index('"_last_event"')
        except ValueError:
            # No last event found, return the original API response
            return api_response

        # Keep original header
        result = [lines[0]]

        for line in lines[1:]:
            if line:
                # Change last event column by ''
                split = line.split(';')
                split[idx] = ''
                result.append(';'.join(split))
            else:
                # Empty line
                result.append(line)
        return "\n".join(result)

    def cleartests(self):
        r = requests.delete(f"{self.api_base}{self.clear_tests_endpoint}")
        if r.status_code != 200:
            self._log_error(f"Error clearing tests")

    def _check_api_output(self, endpoint: str, expect: str, step_name: str):
        def sort_lines(data: str):
            return "\n".join(sorted(data.split("\n")))

        testfile = f"expect.{expect}.ndjson"

        expected_data = sort_lines(self._load_testfile(testfile))
        r = requests.get(f"{self.api_base}{endpoint}")

        if r.status_code != 200:
            self._log_error(f"Error requesting {endpoint}")

        received = sort_lines(self._remove_last_event(r.text))

        if received == expected_data:
            self._log_info(f"{step_name}: OK")
        else:
            self._log_error(f"{step_name}: ERROR")
            self._log_error(f"Expected data: {expected_data}")
            self._log_error(f"Received data: {received}")

    def _log_error(self, message):
        logger.error(message)

    def _log_info(self, message):
        logger.info(message)

    def _load_testfile(self, filename: str):
        """Returns content of test file in expect directory

        """
        with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'expect', filename)) as f:
            return f.read()

    def _import_workflow_definition(self, catalog: str, collection: str, application: str):
        return {
            'type': 'workflow',
            'workflow': IMPORT,
            'header': {
                'catalogue': catalog,
                'collection': collection,
                'application': application,
            }
        }

    def _check_workflow_step_definition(self, endpoint: str, expect: str, description: str):
        return {
            'type': 'workflow_step',
            'step_name': END_TO_END_CHECK,
            'header': {
                'endpoint': endpoint,
                'expect': expect,
                'description': description,
            }
        }

    def _relate_workflow_definition(self, catalog: str, collection: str, attribute: str):
        return {
            'type': 'workflow',
            'workflow': RELATE,
            'header': {
                'catalogue': catalog,
                'collection': collection,
                'attribute': attribute
            }
        }

    def _build_autoid_test_workflow(self):
        workflow = []

        # Import test_entity_autoid's to test autoid issuing
        for source in self.test_import_autoid_sources:
            workflow.append(self._import_workflow_definition(self.test_catalog,
                                                             self.test_import_entity_autoid,
                                                             source))
            workflow.append(self._check_workflow_step_definition(self.check_autoid_endpoint,
                                                                 source,
                                                                 f"Import {source}"))
        return workflow

    def _build_autoid_states_test_workflow(self):
        workflow = []

        # Import test_entity_autoid's to test autoid issuing
        for source in self.test_import_autoid_states_sources:
            workflow.append(self._import_workflow_definition(self.test_catalog,
                                                             self.test_import_entity_autoid_states,
                                                             source))
            workflow.append(self._check_workflow_step_definition(self.check_autoid_states_endpoint,
                                                                 source,
                                                                 f"Import {source}"))
        return workflow

    def _build_import_test_workflow(self):
        workflow = []

        # Import test_entity_ref's to prevent dangling relations
        for source in self.test_import_ref_sources:
            workflow.append(self._import_workflow_definition(self.test_catalog, self.test_import_entity_ref, source))

        # Import test_entity's
        for source in self.test_import_sources:
            workflow.append(self._import_workflow_definition(self.test_catalog, self.test_import_entity, source))
            workflow.append(self._relate_workflow_definition(self.test_catalog, self.test_import_entity,
                                                             self.test_import_entity_reference))
            workflow.append(self._check_workflow_step_definition(self.check_import_endpoint, source,
                                                                 f"Import {source}"))
        return workflow

    def _build_relate_test_workflow(self):
        workflow = []

        for entity in self.test_relation_entities:
            # Import jobs for entities to relate
            workflow.append(self._import_workflow_definition(self.test_catalog, entity, 'REL'))

        for src_entity in self.test_relation_src_entities:
            for dst_rel in self.test_relation_dst_relations:
                # Relate job
                workflow.append(self._relate_workflow_definition(
                    self.test_catalog,
                    src_entity,
                    # Extract relation name from dst_rel (rtc_ref_to_c > ref_to_c)
                    '_'.join(dst_rel.split('_')[1:])
                ))

                rel_entity = f"tst_{self.entities_abbreviations[src_entity]}_tst_{dst_rel}"
                # Check relation
                workflow.append(self._check_workflow_step_definition(
                    f"/dump/rel/{rel_entity}/?format=csv",
                    rel_entity,
                    f"Relation {rel_entity}"
                ))

        return workflow

    def _build_e2e_workflow(self):
        return self._build_autoid_test_workflow() + \
               self._build_autoid_states_test_workflow() +\
               self._build_import_test_workflow() +\
               self._build_relate_test_workflow()

    def get_workflow(self):
        """Receives end-to-end start message.
        Change message header into dynamic workflow.

        :return:
        """

        return self._build_e2e_workflow()

    def check(self, endpoint: str, expect: str, description: str):
        self._check_api_output(endpoint, expect, description)
