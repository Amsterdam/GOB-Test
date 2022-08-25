import datetime

from gobcore.logging.logger import logger

from gobtest.e2e.e2etest import E2ETest


def end_to_end_test_handler(msg):
    """Request to run E2E tests.

    Return message with new generated dynamic workflow in the header.

    :param msg:
    :return:
    """
    now = datetime.datetime.utcnow()
    start_timestamp = int(now.replace(microsecond=0).timestamp())
    header = msg.get('header', {})

    # Set process id before call to logger.configure
    header['process_id'] = header.get('process_id', f"{start_timestamp}.e2e_test")

    logger.configure(msg, 'E2E Test')
    logger.add_message_broker_handler()

    logger.info("Clear any previous test data")

    e2etest = E2ETest(header['process_id'])
    e2etest.cleartests()
    logger.info("Start E2E Test")

    return {
        'header': {
            **header,
            'timestamp': now.isoformat(),
            'workflow': e2etest.get_workflow(),
        },
        'contents': ''
    }


def end_to_end_execute_workflow_handler(msg):
    logger.configure(msg, 'E2E Test')
    logger.add_message_broker_handler()

    workflow_to_execute = msg['header'].get('execute')
    workflow_process_id = msg['header'].get('execute_process_id')
    process_id = msg['header'].get('process_id')

    assert all([workflow_to_execute, workflow_process_id, process_id]), \
        "Expecting attributes 'execute', 'execute_process_id' and 'process_id' in header"

    E2ETest(process_id).execute_workflow(workflow_to_execute, workflow_process_id)

    return {
        'header': {
            **msg.get('header', {}),
        },
        'summary': logger.get_summary(),
    }


def end_to_end_wait_handler(msg):
    logger.configure(msg, 'E2E Test')
    logger.add_message_broker_handler()

    process_id = msg['header'].get('process_id')
    wait_for_process_id = msg['header'].get('wait_for_process_id')
    seconds = msg['header'].get('seconds')

    assert all([process_id, wait_for_process_id, seconds]), \
        "Expecting attributes 'process_id', 'wait_for_process_id' and 'seconds' in header"

    E2ETest(process_id).wait(wait_for_process_id, seconds)

    return {
        'header': {
            **msg.get('header', {}),
        },
        'summary': logger.get_summary(),
    }


def end_to_end_check_handler(msg):
    logger.configure(msg, 'E2E Test')
    logger.add_message_broker_handler()

    endpoint = msg['header'].get('endpoint')
    expect = msg['header'].get('expect')
    description = msg['header'].get('description')
    process_id = msg['header'].get('process_id')

    assert all([endpoint, expect, description, process_id]), \
        "Expecting attributes 'endpoint', 'expect', 'description' and 'process_id' in header"

    E2ETest(process_id).check(endpoint, expect, description)
    return {
        'header': {
            **msg.get('header', {}),
        },
        'summary': logger.get_summary(),
    }
