from gobcore.message_broker.config import (
    END_TO_END_TEST_RESULT_KEY,
    END_TO_END_TEST_QUEUE,
    WORKFLOW_EXCHANGE,
    END_TO_END_CHECK_RESULT_KEY,
    END_TO_END_CHECK_QUEUE,
    DATA_CONSISTENCY_TEST_QUEUE,
    DATA_CONSISTENCY_TEST_RESULT_KEY,
    DATA_CONSISTENCY_TEST,
    END_TO_END_EXECUTE_QUEUE,
    END_TO_END_EXECUTE_RESULT_KEY,
    END_TO_END_WAIT_QUEUE,
    END_TO_END_WAIT_RESULT_KEY
)
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.notifications import get_notification, listen_to_notifications
from gobcore.workflow.start_workflow import start_workflow

from gobtest.e2e.handler import (
    end_to_end_test_handler,
    end_to_end_check_handler,
    end_to_end_execute_workflow_handler,
    end_to_end_wait_handler
)
from gobtest.data_consistency.handler import data_consistency_test_handler, can_handle


def on_dump_listener(msg):
    notification = get_notification(msg)

    workflow = {
        'workflow_name': DATA_CONSISTENCY_TEST
    }

    arguments = {
        'catalogue': notification.header.get('catalogue'),
        'collection': notification.header.get('collection'),
        'application': notification.header.get('application'),
    }

    if can_handle(**arguments):
        arguments['process_id'] = notification.header.get('process_id')
        start_workflow(workflow, arguments)


SERVICEDEFINITION = {
    'e2e_test': {
        'queue': END_TO_END_TEST_QUEUE,
        'handler': end_to_end_test_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': END_TO_END_TEST_RESULT_KEY,
        }
    },
    'e2e_test_check': {
        'queue': END_TO_END_CHECK_QUEUE,
        'handler': end_to_end_check_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': END_TO_END_CHECK_RESULT_KEY,
        }
    },
    'e2e_test_wait': {
        'queue': END_TO_END_WAIT_QUEUE,
        'handler': end_to_end_wait_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': END_TO_END_WAIT_RESULT_KEY,
        }
    },
    'e2e_test_execute_workflow': {
        'queue': END_TO_END_EXECUTE_QUEUE,
        'handler': end_to_end_execute_workflow_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': END_TO_END_EXECUTE_RESULT_KEY,
        }
    },
    'data_consistency_test': {
        'queue': DATA_CONSISTENCY_TEST_QUEUE,
        'handler': data_consistency_test_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': DATA_CONSISTENCY_TEST_RESULT_KEY,
        }
    },
    'data_consistency_test_listener': {
        'queue': lambda: listen_to_notifications("data_consistency_test", "dump"),
        'handler': on_dump_listener
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Test", {"thread_per_service": True})


init()
