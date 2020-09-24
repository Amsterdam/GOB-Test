from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import os
import math
from gobtest.e2e.e2etest import E2ETest, IMPORT, RELATE, END_TO_END_CHECK, END_TO_END_EXECUTE, END_TO_END_WAIT


@patch("gobtest.e2e.e2etest.logger", MagicMock())
class TestE2Test(TestCase):

    def test_init(self):
        e2e = E2ETest('my process id')
        self.assertEqual('my process id', e2e.process_id)

        # Should not be possible to initialise without process id
        with self.assertRaises(TypeError):
            E2ETest()

    def test_remove_last_event(self):
        e2e = E2ETest('process_id')
        inp = "a;b;c\n1;2;3\n4;5;6\n"
        self.assertEqual(inp, e2e._remove_last_event(inp))

        inp = '"a";"b";"c";"_last_event"\n1;2;3;4\n5;6;7;8\n'
        self.assertEqual('"a";"b";"c";"_last_event"\n1;2;3;\n5;6;7;\n', e2e._remove_last_event(inp))

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_check_api_output(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nC"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest('process_id')
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._remove_last_event = MagicMock(side_effect=lambda x: x)
        e2e.api_base = 'API_BASE'

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')
        e2e._remove_last_event.assert_called_with(mock_get.return_value.text)

        mock_get.assert_called_with('API_BASE/some/endpoint')

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_check_api_output_error_status_code(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nC"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 500, 'text': api_result})
        e2e = E2ETest('process_id')
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._remove_last_event = MagicMock(side_effect=lambda x: x)

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_check_api_output_mismatch_result(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nD"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest('process_id')
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._remove_last_event = MagicMock(side_effect=lambda x: x)

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')

    @patch("builtins.open")
    def test_load_testfile(self, mock_open):
        e2e = E2ETest('process_id')

        self.assertEqual(mock_open.return_value.__enter__.return_value.read.return_value,
                         e2e._load_testfile('filename'))

        self.assertTrue(mock_open.call_args[0][0].endswith(os.path.join('expect', 'filename')))

    def test_import_workflow_definition(self):
        self.assertEqual({
            'type': 'workflow',
            'workflow': IMPORT,
            'header': {
                'catalogue': 'cat',
                'collection': 'col',
                'application': 'app',
            }
        }, E2ETest('process_id')._import_workflow_definition('cat', 'col', 'app'))

    def test_wait_step_workflow_definition(self):
        self.assertEqual({
            'type': 'workflow_step',
            'step_name': END_TO_END_WAIT,
            'header': {
                'wait_for_process_id': 'wait process id',
                'seconds': 248024,
            }
        }, E2ETest('process_id')._wait_step_workflow_definition('wait process id', 248024))

    def test_check_workflow_step_definition(self):
        self.assertEqual({
            'type': 'workflow_step',
            'step_name': END_TO_END_CHECK,
            'header': {
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
            }
        }, E2ETest('process_id')._check_workflow_step_definition('endp', 'exp', 'desc'))

    def test_execute_start_workflow_definition(self):
        self.assertEqual({
            'type': 'workflow_step',
            'step_name': END_TO_END_EXECUTE,
            'header': {
                'execute': ['the', 'workflow'],
                'execute_process_id': 'the process id',
            }
        }, E2ETest('process_id')._execute_start_workflow_definition(['the', 'workflow'], 'the process id'))

    def test_relate_workflow_definition(self):
        self.assertEqual({
            'type': 'workflow',
            'workflow': RELATE,
            'header': {
                'catalogue': 'cat',
                'collection': 'col',
                'attribute': 'attr'
            }
        }, E2ETest('process_id')._relate_workflow_definition('cat', 'col', 'attr'))

    def test_build_autoid_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity_autoid = 'autoid entity'
        e2e.test_import_autoid_sources = ['src A', 'src B']
        e2e.check_autoid_endpoint = 'autoid endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda workflow, process_id: (workflow, process_id)
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        expect = [
            (['import test cat,autoid entity,src A'], 'process_id.autoid.0'),
            'wait 10 for process_id.autoid.0',
            'check autoid endpoint,src A,Import src A',
            (['import test cat,autoid entity,src B'], 'process_id.autoid.1'),
            'wait 10 for process_id.autoid.1',
            'check autoid endpoint,src B,Import src B'
        ]
        result = e2e._build_autoid_test_workflow()
        self.assertEqual(result, expect)

    def test_build_autoid_states_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity_autoid_states = 'autoid entity'
        e2e.test_import_autoid_states_sources = ['src A', 'src B']
        e2e.check_autoid_states_endpoint = 'autoid endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda workflow, process_id: (workflow, process_id)
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        expect = [
            (['import test cat,autoid entity,src A'], 'process_id.autoid_states.0'),
            'wait 10 for process_id.autoid_states.0',
            'check autoid endpoint,src A,Import src A',
            (['import test cat,autoid entity,src B'], 'process_id.autoid_states.1'),
            'wait 10 for process_id.autoid_states.1',
            'check autoid endpoint,src B,Import src B'
        ]
        result = e2e._build_autoid_states_test_workflow()
        self.assertEqual(result, expect)

    def test_build_import_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity = 'import entity'
        e2e.test_import_sources = ['src A', 'src B']
        e2e.check_import_endpoint = 'import endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda workflow, process_id: (workflow, process_id)
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        expect = [
            (['import test cat,test_entity_ref,ADD',
              'import test cat,test_entity_ref,MODIFY1',
              'import test cat,import entity,src A',
              'relate test cat,import entity,reference'],
             'process_id.import_test.src A.0'),
            'wait 10 for process_id.import_test.src A.0',
            'check import endpoint,src A,Import src A',
            (['import test cat,import entity,src B',
              'relate test cat,import entity,reference'],
             'process_id.import_test.src B.1'),
            'wait 10 for process_id.import_test.src B.1',
            'check import endpoint,src B,Import src B'
        ]
        result = e2e._build_import_test_workflow()
        self.assertEqual(result, expect)

    def test_build_relate_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e.test_catalog = 'test cat'
        e2e.test_relation_entities = ['rel entity A', 'rel entity B']
        e2e.test_relation_src_entities = ['rel src A', 'rel src B']
        e2e.test_relation_dst_relations = ['rel_dst_a', 'rel_dst_b']
        e2e.entities_abbreviations = {'rel src A': 'src A', 'rel src B': 'src B'}

        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda workflow, process_id: (workflow, process_id)
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        self.assertEqual([
            'import test cat,rel entity A,REL',
            'import test cat,rel entity B,REL',
            (['relate test cat,rel src A,dst_a'], 'process_id.relate.rel src A.rel_dst_a'),
            'wait 10 for process_id.relate.rel src A.rel_dst_a',
            'check /dump/rel/tst_src A_tst_rel_dst_a/?format=csv,tst_src '
            'A_tst_rel_dst_a,Relation tst_src A_tst_rel_dst_a',
            (['relate test cat,rel src A,dst_b'], 'process_id.relate.rel src A.rel_dst_b'),
            'wait 10 for process_id.relate.rel src A.rel_dst_b',
            'check /dump/rel/tst_src A_tst_rel_dst_b/?format=csv,tst_src '
            'A_tst_rel_dst_b,Relation tst_src A_tst_rel_dst_b',
            (['relate test cat,rel src B,dst_a'], 'process_id.relate.rel src B.rel_dst_a'),
            'wait 10 for process_id.relate.rel src B.rel_dst_a',
            'check /dump/rel/tst_src B_tst_rel_dst_a/?format=csv,tst_src '
            'B_tst_rel_dst_a,Relation tst_src B_tst_rel_dst_a',
            (['relate test cat,rel src B,dst_b'], 'process_id.relate.rel src B.rel_dst_b'),
            'wait 10 for process_id.relate.rel src B.rel_dst_b',
            'check /dump/rel/tst_src B_tst_rel_dst_b/?format=csv,tst_src '
            'B_tst_rel_dst_b,Relation tst_src B_tst_rel_dst_b'
        ], e2e._build_relate_test_workflow())

    def test_build_relate_collapsed_states_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda wf, process_id: 'execute ' + process_id
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        self.assertEqual([
            'import test_catalogue,rel_collapsed_a,REL',
            'import test_catalogue,rel_collapsed_b,REL',
            'execute process_id.relate.collapsed_states',
            'wait 10 for process_id.relate.collapsed_states',
            'check '
            '/dump/rel/tst_cola_tst_colb_reference/?format=csv,tst_cola_tst_colb_reference,Relation '
            'tst_cola_tst_colb_reference'
        ], e2e._build_relate_collapsed_states_test_workflow())

    def test_build_relate_multiple_allowed_test_workflow(self):
        e2e = E2ETest('process_id')
        e2e.test_catalog = 'test cat'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)
        e2e._execute_start_workflow_definition = lambda wf, process_id: 'execute_' + process_id
        e2e._wait_step_workflow_definition = lambda process_id, seconds=10: f"wait {seconds} for {process_id}"

        # Test only for length. Is a long test.
        self.assertEqual(36, len(e2e._build_relate_multiple_allowed_test_workflow()))

    def test_build_e2e_workflow(self):
        e2e = E2ETest('process_id')
        e2e._build_autoid_test_workflow = MagicMock(return_value=['0', '1'])
        e2e._build_autoid_states_test_workflow = MagicMock(return_value=['2', '3'])
        e2e._build_import_test_workflow = MagicMock(return_value=['a', 'b'])
        e2e._build_relate_test_workflow = MagicMock(return_value=['c', 'd'])
        e2e._build_relate_multiple_allowed_test_workflow = MagicMock(return_value=['e', 'f'])
        e2e._build_relate_collapsed_states_test_workflow = MagicMock(return_value=['g', 'h'])
        self.assertEqual(['0', '1', '2', '3', 'a', 'b', 'c', 'd', 'g', 'h', 'e', 'f'], e2e._build_e2e_workflow())

    def test_get_workflow(self):
        e2e = E2ETest('process_id')
        e2e._build_e2e_workflow = MagicMock()
        self.assertEqual(e2e._build_e2e_workflow.return_value, e2e.get_workflow())

    @patch("gobtest.e2e.e2etest.start_workflow")
    def test_execute_workflow(self, mock_start_workflow):
        e2e = E2ETest('process_id')

        e2e.execute_workflow(['some', 'workflow'], 'some process id')

        mock_start_workflow.assert_called_with(
            {'workflow_name': 'dynamic'},
            {'header': {
                'workflow': ['some', 'workflow'],
                'process_id': 'some process id',
            }}
        )

    def test_check(self):
        e2e = E2ETest('process_id')
        e2e._check_api_output = MagicMock()
        e2e.check('a', 'b', 'c')

        e2e._check_api_output.assert_called_with('a', 'b', 'c')

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_pending_messages(self, mock_get):
        e2e = E2ETest('process_id')
        mock_get.return_value.ok = True

        mock_get.return_value.json = lambda: [
            {'name': 'q1', 'messages_unacknowledged': 1},
            {'name': 'q1.some.thing', 'messages_unacknowledged': 3},
            {'name': 'q2', 'messages_unacknowledged': 5},
            {'name': 'some.q2', 'messages_unacknowledged': 7},
            {'name': 'q3', 'messages_unacknowledged': 11}
        ]
        pending = e2e.pending_messages()
        self.assertEqual(pending, 1 + 3 + 5 + 7 + 11)

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_pending_jobs(self, mock_get):
        e2e = E2ETest('process_id')
        mock_get.return_value.ok = True

        mock_get.return_value.json = lambda: []
        pending = e2e.pending_jobs('p1')
        self.assertEqual(pending, -1)

        mock_get.return_value.json = lambda: [
                    {'jobid': 1, 'processId': 'p1', 'status': 'scheduled'},
                    {'jobid': 1, 'processId': 'p1', 'status': 'any status'},
                    {'jobid': 1, 'processId': 'p1', 'status': 'ended'},
                    {'jobid': 1, 'processId': 'p1', 'status': 'rejected'},
                    {'jobid': 1, 'processId': 'p1', 'status': 'started'},
                    {'jobid': 1, 'processId': 'p1', 'status': 'scheduled'}
                ]
        # A job is pending when it has not started of was rejected
        pending = e2e.pending_jobs('p1')
        self.assertEqual(pending, 4)

    @patch("gobtest.e2e.e2etest.time.sleep")
    def test_wait(self, mock_sleep):
        e2e = E2ETest('process_id')
        mock_pending_messages = MagicMock()
        mock_pending_jobs = MagicMock()

        e2e.pending_messages = mock_pending_messages
        e2e.pending_jobs = mock_pending_jobs

        max_seconds_to_try = 99

        # No pending jobs and messages, process has finished
        mock_pending_jobs.return_value = 0
        mock_pending_messages.return_value = 0
        result = e2e.wait('any process id', max_seconds_to_try)
        self.assertEqual(result, True)
        self.assertEqual(mock_sleep.call_count, 1)  # 1 confirmation

        # No pending jobs and pending messages, process has finished
        mock_sleep.reset_mock()
        mock_pending_jobs.return_value = 0
        mock_pending_messages.return_value = 1
        result = e2e.wait('any process id', max_seconds_to_try)
        self.assertEqual(result, True)
        self.assertEqual(mock_sleep.call_count, 2)  # 1 extra confirmation for pending messages

        # pending jobs and pending messages, process has not finished
        mock_sleep.reset_mock()
        mock_pending_jobs.return_value = 1
        mock_pending_messages.return_value = 1
        result = e2e.wait('any process id', max_seconds_to_try)
        self.assertEqual(result, False)
        self.assertEqual(mock_sleep.call_count,
                         math.ceil(max_seconds_to_try/e2e.CHECK_EVERY_N_SECONDS_FOR_PROCESS_TO_FINISH))

        # pending jobs and no pending messages, process has not finished
        mock_sleep.reset_mock()
        mock_pending_jobs.return_value = 1
        mock_pending_messages.return_value = 0
        result = e2e.wait('any process id', max_seconds_to_try)
        self.assertEqual(result, False)
        self.assertEqual(mock_sleep.call_count,
                         math.ceil(max_seconds_to_try/e2e.CHECK_EVERY_N_SECONDS_FOR_PROCESS_TO_FINISH))

    @patch("gobtest.e2e.e2etest.requests.delete")
    def test_cleartests(self, mock_delete):
        mock_delete.return_value.status_code = 200
        e2e = E2ETest('process_id')
        e2e._log_error = MagicMock()
        e2e.cleartests()
        mock_delete.assert_called()
        e2e._log_error.assert_not_called()

        mock_delete.return_value.status_code = 'any error code'
        e2e.cleartests()
        e2e._log_error.assert_called()
