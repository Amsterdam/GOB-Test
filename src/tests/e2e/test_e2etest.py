from unittest import TestCase
from unittest.mock import patch, MagicMock

import os
from gobtest.e2e.e2etest import E2ETest, IMPORT, RELATE, END_TO_END_CHECK


@patch("gobtest.e2e.e2etest.logger", MagicMock())
class TestE2Test(TestCase):

    def test_remove_last_event(self):
        e2e = E2ETest()
        inp = "a;b;c\n1;2;3\n4;5;6\n"
        self.assertEqual(inp, e2e._remove_last_event(inp))

        inp = '"a";"b";"c";"_last_event"\n1;2;3;4\n5;6;7;8\n'
        self.assertEqual('"a";"b";"c";"_last_event"\n1;2;3;\n5;6;7;\n', e2e._remove_last_event(inp))

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_check_api_output(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nC"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest()
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
        e2e = E2ETest()
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._remove_last_event = MagicMock(side_effect=lambda x: x)

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')

    @patch("gobtest.e2e.e2etest.requests.get")
    def test_check_api_output_mismatch_result(self, mock_get):
        api_result = "A\nB\nC"
        expected_result = "B\nA\nD"

        mock_get.return_value = type('MockResponse', (object,), {'status_code': 200, 'text': api_result})
        e2e = E2ETest()
        e2e._load_testfile = MagicMock(return_value=expected_result)
        e2e._remove_last_event = MagicMock(side_effect=lambda x: x)

        e2e._check_api_output('/some/endpoint', 'some testfile', 'Test API Output')

    @patch("builtins.open")
    def test_load_testfile(self, mock_open):
        e2e = E2ETest()

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
        }, E2ETest()._import_workflow_definition('cat', 'col', 'app'))

    def test_check_workflow_step_definition(self):
        self.assertEqual({
            'type': 'workflow_step',
            'step_name': END_TO_END_CHECK,
            'header': {
                'endpoint': 'endp',
                'expect': 'exp',
                'description': 'desc',
            }
        }, E2ETest()._check_workflow_step_definition('endp', 'exp', 'desc'))

    def test_relate_workflow_definition(self):
        self.assertEqual({
            'type': 'workflow',
            'workflow': RELATE,
            'header': {
                'catalogue': 'cat',
                'collection': 'col',
                'attribute': 'attr'
            }
        }, E2ETest()._relate_workflow_definition('cat', 'col', 'attr'))

    def test_build_autoid_test_workflow(self):
        e2e = E2ETest()
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity_autoid = 'autoid entity'
        e2e.test_import_autoid_sources = ['src A', 'src B']
        e2e.check_autoid_endpoint = 'autoid endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)

        expect = [
            'import test cat,autoid entity,src A',
            'check autoid endpoint,src A,Import src A',
            'import test cat,autoid entity,src B',
            'check autoid endpoint,src B,Import src B'
        ]
        result = e2e._build_autoid_test_workflow()
        print(result)
        self.assertEqual(result, expect)

    def test_build_autoid_states_test_workflow(self):
        e2e = E2ETest()
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity_autoid_states = 'autoid entity'
        e2e.test_import_autoid_states_sources = ['src A', 'src B']
        e2e.check_autoid_states_endpoint = 'autoid endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)

        expect = [
            'import test cat,autoid entity,src A',
            'check autoid endpoint,src A,Import src A',
            'import test cat,autoid entity,src B',
            'check autoid endpoint,src B,Import src B'
        ]
        result = e2e._build_autoid_states_test_workflow()
        print(result)
        self.assertEqual(result, expect)

    def test_build_import_test_workflow(self):
        e2e = E2ETest()
        e2e.test_catalog = 'test cat'
        e2e.test_import_entity = 'import entity'
        e2e.test_import_sources = ['src A', 'src B']
        e2e.check_import_endpoint = 'import endpoint'
        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)

        expect = [
            'import test cat,test_entity_ref,ADD',
            'import test cat,test_entity_ref,MODIFY1',
            "import test cat,import entity,src A",
            "relate test cat,import entity,reference",
            "check import endpoint,src A,Import src A",
            "import test cat,import entity,src B",
            "relate test cat,import entity,reference",
            "check import endpoint,src B,Import src B",
        ]
        result = e2e._build_import_test_workflow()
        self.assertEqual(result, expect)

    def test_build_relate_test_workflow(self):
        e2e = E2ETest()
        e2e.test_catalog = 'test cat'
        e2e.test_relation_entities = ['rel entity A', 'rel entity B']
        e2e.test_relation_src_entities = ['rel src A', 'rel src B']
        e2e.test_relation_dst_relations = ['rel_dst_a', 'rel_dst_b']
        e2e.entities_abbreviations = {'rel src A': 'src A', 'rel src B': 'src B'}

        e2e._import_workflow_definition = lambda *args: "import " + ",".join(args)
        e2e._check_workflow_step_definition = lambda *args: "check " + ",".join(args)
        e2e._relate_workflow_definition = lambda *args: "relate " + ",".join(args)

        self.assertEqual([
            "import test cat,rel entity A,REL",
            "import test cat,rel entity B,REL",
            "relate test cat,rel src A,dst_a",
            "check /dump/rel/tst_src A_tst_rel_dst_a/?format=csv,tst_src A_"
            "tst_rel_dst_a,Relation tst_src A_tst_rel_dst_a",
            "relate test cat,rel src A,dst_b",
            "check /dump/rel/tst_src A_tst_rel_dst_b/?format=csv,tst_src A_"
            "tst_rel_dst_b,Relation tst_src A_tst_rel_dst_b",
            "relate test cat,rel src B,dst_a",
            "check /dump/rel/tst_src B_tst_rel_dst_a/?format=csv,tst_src B_"
            "tst_rel_dst_a,Relation tst_src B_tst_rel_dst_a",
            "relate test cat,rel src B,dst_b",
            "check /dump/rel/tst_src B_tst_rel_dst_b/?format=csv,tst_src B_"
            "tst_rel_dst_b,Relation tst_src B_tst_rel_dst_b",
        ], e2e._build_relate_test_workflow())

    def test_build_e2e_workflow(self):
        e2e = E2ETest()
        e2e._build_autoid_test_workflow = MagicMock(return_value=['0', '1'])
        e2e._build_autoid_states_test_workflow = MagicMock(return_value=['2', '3'])
        e2e._build_import_test_workflow = MagicMock(return_value=['a', 'b'])
        e2e._build_relate_test_workflow = MagicMock(return_value=['c', 'd'])
        self.assertEqual(['0', '1', '2', '3', 'a', 'b', 'c', 'd'], e2e._build_e2e_workflow())

    def test_get_workflow(self):
        e2e = E2ETest()
        e2e._build_e2e_workflow = MagicMock()
        self.assertEqual(e2e._build_e2e_workflow.return_value, e2e.get_workflow())

    def test_check(self):
        e2e = E2ETest()
        e2e._check_api_output = MagicMock()

        e2e.check('a', 'b', 'c')
        e2e._check_api_output.assert_called_with('a', 'b', 'c')

    @patch("gobtest.e2e.e2etest.requests.delete")
    def test_cleartests(self, mock_delete):
        mock_delete.return_value.status_code = 200
        e2e = E2ETest()
        e2e._log_error = MagicMock()
        e2e.cleartests()
        mock_delete.assert_called()
        e2e._log_error.assert_not_called()

        mock_delete.return_value.status_code = 'any error code'
        e2e.cleartests()
        e2e._log_error.assert_called()
