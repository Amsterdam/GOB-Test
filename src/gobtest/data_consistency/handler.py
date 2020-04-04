import datetime

from gobcore.logging.logger import logger

from gobtest.data_consistency.data_consistency_test import DataConsistencyTest


def data_consistency_test_handler(msg):
    """Request to run data consistency tests.

    :param msg:
    :return:
    """
    logger.configure(msg, 'Data consistency E2E test')
    logger.info("Start data consistency test")

    catalog = msg['header'].get('catalogue')
    collection = msg['header'].get('collection')
    application = msg['header'].get('application')

    assert all([catalog, collection]), "Expecting header attributes 'catalogue' and 'collection'"

    # No return value. Results are captured by logger.
    DataConsistencyTest(catalog, collection, application).run()

    return {
        'header': {
            **msg.get('header', {}),
            'timestamp': datetime.datetime.utcnow().isoformat(),
        },
        'summary': {
            'warnings': logger.get_warnings(),
            'errors': logger.get_errors(),
        }
    }
