from gobcore.message_broker.config import (
    DATA_CONSISTENCY_TEST,
    DATA_CONSISTENCY_TEST_QUEUE,
    DATA_CONSISTENCY_TEST_RESULT_KEY,
    END_TO_END_CHECK_QUEUE,
    END_TO_END_CHECK_RESULT_KEY,
    END_TO_END_EXECUTE_QUEUE,
    END_TO_END_EXECUTE_RESULT_KEY,
    END_TO_END_TEST_QUEUE,
    END_TO_END_TEST_RESULT_KEY,
    END_TO_END_WAIT_QUEUE,
    END_TO_END_WAIT_RESULT_KEY,
    WORKFLOW_EXCHANGE,
)
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.notifications import get_notification, listen_to_notifications
from gobcore.message_broker.typing import ServiceDefinition
from gobcore.workflow.start_workflow import start_workflow

from gobtest.data_consistency.handler import can_handle, data_consistency_test_handler
from gobtest.e2e.handler import (
    end_to_end_check_handler,
    end_to_end_execute_workflow_handler,
    end_to_end_test_handler,
    end_to_end_wait_handler,
)


def on_events_listener(msg):
    """On events listener."""
    notification = get_notification(msg)

    workflow = {"workflow_name": DATA_CONSISTENCY_TEST}

    arguments = {
        "catalogue": notification.header.get("catalogue"),
        "collection": notification.header.get("collection"),
        "application": notification.header.get("application"),
    }

    if can_handle(**arguments):
        arguments["process_id"] = notification.header.get("process_id")
        start_workflow(workflow, arguments)


SERVICEDEFINITION: ServiceDefinition = {
    "e2e_test": {
        "queue": END_TO_END_TEST_QUEUE,
        "handler": end_to_end_test_handler,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": END_TO_END_TEST_RESULT_KEY,
        },
    },
    "e2e_test_check": {
        "queue": END_TO_END_CHECK_QUEUE,
        "handler": end_to_end_check_handler,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": END_TO_END_CHECK_RESULT_KEY,
        },
    },
    "e2e_test_wait": {
        "queue": END_TO_END_WAIT_QUEUE,
        "handler": end_to_end_wait_handler,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": END_TO_END_WAIT_RESULT_KEY,
        },
    },
    "e2e_test_execute_workflow": {
        "queue": END_TO_END_EXECUTE_QUEUE,
        "handler": end_to_end_execute_workflow_handler,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": END_TO_END_EXECUTE_RESULT_KEY,
        },
    },
    "data_consistency_test": {
        "queue": DATA_CONSISTENCY_TEST_QUEUE,
        "handler": data_consistency_test_handler,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": DATA_CONSISTENCY_TEST_RESULT_KEY,
        },
    },
    "data_consistency_test_listener": {
        "queue": lambda: listen_to_notifications("data_consistency_test", "events"),
        "handler": on_events_listener,
    },
}


def init():
    """Start messagedriven service."""
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Test", {"thread_per_service": True})


init()
