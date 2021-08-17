"""Calling this module will run the end-to-end tests.

The end-to-end test tests importing, comparing, storing and relating of entities.

Program exit code is the number of errors that occurred, or -1 in case a system error occurred.

"""

import os
import requests
import time

from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT, END_TO_END_CHECK, RELATE, END_TO_END_EXECUTE, END_TO_END_WAIT
from gobcore.workflow.start_workflow import start_workflow
from gobtest.config import API_HOST, MANAGEMENT_API_PUBLIC_BASE


class E2ETest:
    """Class E2ETest

    Tests importing, comparing, updating and relating of entities. Uses GOB-API to verify the correctness of the
    result.

    """
    MAX_SECONDS_TO_WAIT_FOR_PROCESS_TO_FINISH = 10 * 60  # Wait for maximally 10 minutes
    CHECK_EVERY_N_SECONDS_FOR_PROCESS_TO_FINISH = 5      # Check every 5 seconds

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

    api_base = f"{API_HOST}/gob/public"

    clear_tests_endpoint = "/alltests/"
    check_import_endpoint = "/test_catalogue/test_entity/?ndjson=true"
    check_autoid_endpoint = "/test_catalogue/test_entity_autoid/?ndjson=true"
    check_autoid_states_endpoint = "/test_catalogue/test_entity_autoid_states/?ndjson=true"

    def __init__(self, process_id: str):
        self.process_id = process_id

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
            self._log_error("Error clearing tests")

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
            self._log_error(f"{step_name}: ERROR checking {testfile} with {endpoint}")
            self._log_error(f"Expected data: {expected_data}")
            self._log_error(f"Received data: {received}")

    def _log_error(self, message):
        logger.error(message)

    def _log_warning(self, message):
        logger.warning(message)

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

    def _wait_step_workflow_definition(self, wait_for_process_id: str,
                                       seconds: int = MAX_SECONDS_TO_WAIT_FOR_PROCESS_TO_FINISH):
        return {
            'type': 'workflow_step',
            'step_name': END_TO_END_WAIT,
            'header': {
                'wait_for_process_id': wait_for_process_id,
                'seconds': seconds,
            }
        }

    def _check_workflow_step_definition(self, endpoint: str, expect: str, description: str):
        """Compares the output from an API endpoint with a file with the expected output.
        Waits for the process with :check_process_id: to finish.

        :param endpoint: The endpoint response to check
        :param expect: The file containing the expected response
        :param description: Some descriptive string
        :param check_process_id: The ID of the process to wait for before checking the endpoint.
        :return:
        """
        return {
            'type': 'workflow_step',
            'step_name': END_TO_END_CHECK,
            'header': {
                'endpoint': endpoint,
                'expect': expect,
                'description': description,
            }
        }

    def _execute_start_workflow_definition(self, workflow: list, process_id: str):
        """Start a workflow as a separate job. Assign given :process_id: .
        Allows us to wait for the complete process including event-triggered jobs to finish before checking the
        results.

        :param workflow: The dynamic workflow to execute
        :param process_id: The process id to assign to the new workflow (with which we can keep track of it)
        :return:
        """

        return {
            'type': 'workflow_step',
            'step_name': END_TO_END_EXECUTE,
            'header': {
                'execute': workflow,
                'execute_process_id': process_id
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
        for i, source in enumerate(self.test_import_autoid_sources):
            process_id = f"{self.process_id}.autoid.{i}"
            workflow.append(
                self._execute_start_workflow_definition(
                    [
                        self._import_workflow_definition(self.test_catalog,
                                                         self.test_import_entity_autoid,
                                                         source)
                    ],
                    process_id
                )
            )

            workflow.append(self._wait_step_workflow_definition(process_id))
            workflow.append(self._check_workflow_step_definition(self.check_autoid_endpoint,
                                                                 source,
                                                                 f"Import {source}"))
        return workflow

    def _build_autoid_states_test_workflow(self):
        workflow = []

        # Import test_entity_autoid's to test autoid issuing
        for i, source in enumerate(self.test_import_autoid_states_sources):
            process_id = f"{self.process_id}.autoid_states.{i}"

            workflow.append(
                self._execute_start_workflow_definition(
                    [self._import_workflow_definition(self.test_catalog,
                                                      self.test_import_entity_autoid_states,
                                                      source)],
                    process_id
                )
            )

            workflow.append(self._wait_step_workflow_definition(process_id))
            workflow.append(self._check_workflow_step_definition(self.check_autoid_states_endpoint,
                                                                 source,
                                                                 f"Import {source}"))
        return workflow

    def _build_import_test_workflow(self):
        workflow = []

        sub_workflow = []
        # Import test_entity_ref's to prevent dangling relations
        for source in self.test_import_ref_sources:
            sub_workflow.append(
                self._import_workflow_definition(self.test_catalog, self.test_import_entity_ref, source)
            )

        # Import test_entity's
        for i, source in enumerate(self.test_import_sources):
            subworkflow_process_id = f"{self.process_id}.import_test.{source}.{i}"

            sub_workflow.append(self._import_workflow_definition(self.test_catalog, self.test_import_entity, source))
            sub_workflow.append(self._relate_workflow_definition(self.test_catalog, self.test_import_entity,
                                                                 self.test_import_entity_reference))

            workflow.append(self._execute_start_workflow_definition(sub_workflow, subworkflow_process_id))
            sub_workflow = []

            workflow.append(self._wait_step_workflow_definition(subworkflow_process_id))
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
                process_id = f"{self.process_id}.relate.{src_entity}.{dst_rel}"

                # Relate job
                workflow.append(
                    self._execute_start_workflow_definition(
                        [
                            self._relate_workflow_definition(
                                self.test_catalog,
                                src_entity,
                                # Extract relation name from dst_rel (rtc_ref_to_c > ref_to_c)
                                '_'.join(dst_rel.split('_')[1:])

                            )
                        ],
                        process_id
                    )
                )

                rel_entity = f"tst_{self.entities_abbreviations[src_entity]}_tst_{dst_rel}"
                # Check relation
                workflow.append(self._wait_step_workflow_definition(process_id))
                workflow.append(self._check_workflow_step_definition(
                    f"/dump/rel/{rel_entity}/?format=csv",
                    rel_entity,
                    f"Relation {rel_entity}"
                ))

        return workflow

    def _build_relate_collapsed_states_test_workflow(self):
        """Builds relate collapsed states test. Collapsed states are states where begin_geldigheid = eind_geldigheid

        :return:
        """
        workflow = []

        src_entity = 'rel_collapsed_a'
        dst_entity = 'rel_collapsed_b'
        relation_attribute = 'reference'
        process_id = f"{self.process_id}.relate.collapsed_states"
        relation_name = "tst_cola_tst_colb_reference"

        workflow.append(self._import_workflow_definition(self.test_catalog, src_entity, 'REL'))
        workflow.append(self._import_workflow_definition(self.test_catalog, dst_entity, 'REL'))

        workflow.append(self._execute_start_workflow_definition([
            self._relate_workflow_definition(
                self.test_catalog,
                src_entity,
                relation_attribute,
            )
        ], process_id))

        workflow.append(self._wait_step_workflow_definition(process_id))
        workflow.append(self._check_workflow_step_definition(
            f"/dump/rel/{relation_name}/?format=csv",
            relation_name,
            f"Relation {relation_name}"
        ))

        return workflow

    def _build_relate_multiple_allowed_test_workflow(self):  # noqa: C901
        """Tests the relate process when multiple_allowed = true is used in gobsources

        Two distinct situations are tested:
        - There is only 1 source, with multiple_allowed = true
        - There are multiple sources, with mixed values for multiple_allowed = true

        For these two situations, the following actions are tested:
        A. The initial relate process
        B. Addition of a new src object that should be related
        C. Deletion of a src object that was related
        D. Re-adding the previously deleted object that should be related
        E. Addition of a bronwaarde that should be related
        F. Deletion of a bronwaarde that was related
        G. Re-adding the previously deleted bronwaarde that should be related
        H. Adding a dst object that should be related
        I. Deleting a dst object that was related
        J. Changing the referenced attribute of a related object
        K. Re-adding a dst object that should be related

        :return:
        """
        src_entity = 'rel_multiple_allowed_src'
        src_multisource_entity = 'rel_multiple_allowed_multisource_src'
        dst_entity = 'rel_multiple_allowed_dst'

        src_entity_relations = [
            'tst_ma1_tst_ma3_manyreference',
            'tst_ma1_tst_ma3_reference'
        ]
        src_multisource_entity_relations = [
            'tst_ma2_tst_ma3_manyreference',
            'tst_ma2_tst_ma3_reference'
        ]
        workflow = []

        """
        Tests are defined by the steps list.

        Meaning of the parameters:
        step no:                this number corresponds with the steps as listed below
        import src:             if true, the src_entity is imported, from source MAsrcA{step_no}
        import multisource src: if true, the src_multisource_entity is imported, from sources MAsrcA{step_no}
                                and MAsrcB{step_no}
        import dst:             if true, dst_entity is imported, from source MAdst{step_no}

        Relates are triggered for the relations that may be updated and the results are checked for these relations.

        The expect filenames are of the form {relation_name}_{step_no}.

        It follows that for every step the appropriate import definitions and correct expect files should be defined.

        Steps with tested cases (see docblock of this method):
        1. Case A. Initial relation of two simple src objects. One object with one source with multiple_allowed = true.
                   The other object has two sources, only one of which with multiple_allowed = true. The result is a
                   hybrid relation table.
        2. Case B. Objects are added to both src tables.
        3. Case C. One of the objects in each table is deleted.
           Case E. A new bronwaarde is added to the manyref of the other object
           Case F. A bronwaarde is removed from the manyref of this same object.
           Case E/F. The bronwaarde in the single ref of this same object is changed.
        4. Case D. Previously deleted objects are added again (from the previous step)
           Case G. Previously deleted bronwaarde is added again (from the previous step)
        5. Case H. A new dst object is added.
           Case I. A dst object is deleted
           Case J. An existing referenced attribute is changed in the dst object.
        6. Case K. The previously deleted dst object is re-added again.
        """
        steps = [
            # (step no, import src?, import multisource_src?, import dst?)
            (1, True, True, True),
            (2, True, True, False),
            (3, True, True, False),
            (4, True, True, False),
            (5, False, False, True),
            (6, False, False, True),
        ]

        for step_no, import_src, import_multisource_src, import_dst in steps:
            relates = []
            check_results = []
            subworkflow = []

            process_id = f"{self.process_id}.relate_multiple_allowed.step{step_no}"

            if import_src:
                subworkflow.append(self._import_workflow_definition(self.test_catalog, src_entity, f'MAsrcA{step_no}'))

            if import_multisource_src:
                subworkflow.append(self._import_workflow_definition(self.test_catalog, src_multisource_entity,
                                                                    f'MAsrcA{step_no}'))
                subworkflow.append(self._import_workflow_definition(self.test_catalog, src_multisource_entity,
                                                                    f'MAsrcB{step_no}'))

            if import_dst:
                subworkflow.append(self._import_workflow_definition(self.test_catalog, dst_entity, f'MAdst{step_no}'))

            if import_src or import_dst:
                relates.append(
                    self._relate_workflow_definition(self.test_catalog, src_entity, 'reference'))
                relates.append(
                    self._relate_workflow_definition(self.test_catalog, src_entity, 'manyreference'))

                check_results += src_entity_relations

            if import_multisource_src or import_dst:
                relates.append(
                    self._relate_workflow_definition(self.test_catalog, src_multisource_entity, 'reference'))
                relates.append(
                    self._relate_workflow_definition(self.test_catalog, src_multisource_entity, 'manyreference'))

                check_results += src_multisource_entity_relations

            subworkflow += relates

            workflow.append(self._execute_start_workflow_definition(subworkflow, process_id))
            workflow.append(self._wait_step_workflow_definition(process_id,
                                                                self.MAX_SECONDS_TO_WAIT_FOR_PROCESS_TO_FINISH))

            for check_result in check_results:
                workflow.append(self._check_workflow_step_definition(
                    f"/dump/rel/{check_result}/?format=csv&exclude_deleted=true",
                    f"{check_result}_{step_no}",
                    f"Relation {check_result}",
                ))

        return workflow

    def _build_e2e_workflow(self):
        return self._build_autoid_test_workflow() + \
               self._build_autoid_states_test_workflow() +\
               self._build_import_test_workflow() +\
               self._build_relate_test_workflow() +\
               self._build_relate_collapsed_states_test_workflow() +\
               self._build_relate_multiple_allowed_test_workflow()

    def get_workflow(self):
        """Receives end-to-end start message.
        Change message header into dynamic workflow.

        :return:
        """

        return self._build_e2e_workflow()

    def execute_workflow(self, workflow: list, workflow_process_id: str):
        args = {
            'header': {
                'workflow': workflow,
                'process_id': workflow_process_id,
            }
        }
        workflow = {'workflow_name': 'dynamic'}

        start_workflow(workflow, args)

    def pending_messages(self):
        """
        Reports the number of pending messages for queues that contain notifications or start workflows

        :return: #pending messages
        """
        url = f"{MANAGEMENT_API_PUBLIC_BASE}/state/workflow"
        response = requests.get(url)
        assert response.ok, "API request for pending workflow messages has failed"
        workflow_queues = response.json()
        # Example response
        # [{'name': ..., 'messages_unacknowledged': ...}, {...}]

        # Count pending messages
        return sum([queue['messages_unacknowledged'] for queue in workflow_queues])

    def pending_jobs(self, process_id):
        """
        Reports the number of jobs that run for the given process

        If no jobs are found -1 is returned to indicate that the process has not yet started or does not exist

        :param process_id:
        :return:
        """
        url = f"{MANAGEMENT_API_PUBLIC_BASE}/state/process/{process_id}"
        response = requests.get(url)
        assert response.ok, "API request for process state has failed"
        jobs = response.json()
        # Example response
        # [{'id': 226, 'status': 'scheduled'}]
        if not jobs:
            # Process has not yet started or does not exist
            return -1
        # Process has started, return number of jobs that do not yet have finished
        # A job is finished when it has ended or it has never started (rejected)
        end_states = ['ended', 'rejected']
        unfinished_jobs = [job for job in jobs if not job['status'] in end_states]
        return len(unfinished_jobs)

    def wait(self, process_id: str, max_seconds_to_try: int):
        """Wait for :process_id: to be finished.

        The check is quite straightforward
        First the jobs for the given process id are requested
        If there are any jobs the process is considered to have started
        If all jobs have ended the process is considered to have ended

        An extra check is made to check the length of the notification queues and start workflow queue
        If any messages are present in any of these queues the process will require more confirmations
        that all jobs have ended

        The given number of seconds is the max time that the wait process will check for jobs

        :param process_id: the process id of the process to wait for
        :param max_seconds_to_try: the max time to check for running jobs within the process
        :return:
        """
        self._log_info(f"Wait for process {process_id} to complete for max {max_seconds_to_try} seconds")

        confirmed = 0                           # Number of times the process has been confirmed to have finished
        last_pending_jobs = 0                   # Last number of pending jobs that has been registered
        seconds_to_try = max_seconds_to_try     # Max nr seconds to wait for process to have finished
        while seconds_to_try > 0:
            # Try for a maximum of seconds_to_try seconds

            # count pending jobs for the given process
            pending_jobs = self.pending_jobs(process_id)

            if pending_jobs > last_pending_jobs:
                # Process is still expanding
                seconds_to_try = max_seconds_to_try
            last_pending_jobs = pending_jobs

            if pending_jobs == 0:
                # No pending jobs
                if confirmed >= 1:
                    # If sufficiently confirmed then consider process as finished
                    self._log_info(f"Process {process_id} has completed")
                    return True

                # count pending notifications or workflow starts
                pending_messages = self.pending_messages()

                if pending_messages == 0:
                    # No pending jobs and no pending messages
                    # Confirmation is required because of a possible race condition
                    # - Workflow has accepted the message but not yet committed the job
                    confirmed += 1
                else:
                    # No pending jobs but there are pending messages
                    # Require extra confirmations
                    confirmed += 0.5
            else:
                confirmed = 0   # process still running; reset any confirmations

            # Wait some time before re-testing the process
            time.sleep(self.CHECK_EVERY_N_SECONDS_FOR_PROCESS_TO_FINISH)
            # But do not test forever
            seconds_to_try -= self.CHECK_EVERY_N_SECONDS_FOR_PROCESS_TO_FINISH

        self._log_warning(f"Max wait time for process {process_id} to complete exceeded.")
        return False

    def check(self, endpoint: str, expect: str, description: str):
        """

        :param endpoint:
        :param expect:
        :param description:
        :return:
        """
        self._check_api_output(endpoint, expect, description)
