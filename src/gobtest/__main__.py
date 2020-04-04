from gobcore.message_broker.config import (
    END_TO_END_TEST_RESULT_KEY,
    END_TO_END_TEST_QUEUE,
    WORKFLOW_EXCHANGE,
    END_TO_END_CHECK_RESULT_KEY,
    END_TO_END_CHECK_QUEUE,
    DATA_CONSISTENCY_TEST_QUEUE,
    DATA_CONSISTENCY_TEST_RESULT_KEY
)
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobtest.e2e.handler import end_to_end_test_handler, end_to_end_check_handler
from gobtest.data_consistency.handler import data_consistency_test_handler


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
    'data_consistency_test': {
        'queue': DATA_CONSISTENCY_TEST_QUEUE,
        'handler': data_consistency_test_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': DATA_CONSISTENCY_TEST_RESULT_KEY,
        }
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Test")


init()
