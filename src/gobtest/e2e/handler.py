import datetime

from gobcore.logging.logger import logger

from gobtest.e2e.e2etest import E2ETest


def end_to_end_test_handler(msg):
    """Request to run E2E tests.

    Return message with new generated dynamic workflow in the header.

    :param msg:
    :return:
    """
    logger.configure(msg, 'E2E Test')
    logger.info("Start E2E Test")
    return {
        'header': {
            **msg.get('header', {}),
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'workflow': E2ETest().get_workflow()
        },
        'contents': ''
    }


def end_to_end_check_handler(msg):
    logger.configure(msg, 'E2E Test')

    endpoint = msg['header'].get('endpoint')
    expect = msg['header'].get('expect')
    description = msg['header'].get('description')

    assert all([endpoint, expect, description]), \
        "Expecting attributes 'endpoint', 'expect' and 'description' in header"

    E2ETest().check(endpoint, expect, description)

    return {
        'header': {
            **msg.get('header', {}),
        },
        'summary': {
            'warnings': logger.get_warnings(),
            'errors': logger.get_errors(),
        }
    }
