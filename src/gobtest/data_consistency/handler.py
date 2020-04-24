import datetime

from gobcore.logging.logger import logger

from gobtest.data_consistency.data_consistency_test import DataConsistencyTest


def data_consistency_test_handler(msg):
    """Request to run data consistency tests.

    :param msg:
    :return:
    """
    catalog = msg['header'].get('catalogue')
    collection = msg['header'].get('collection')
    application = msg['header'].get('application')
    msg['header']['entity'] = msg['header'].get('entity', collection)

    logger.configure(msg, 'Data consistency test')

    assert all([catalog, collection]), "Expecting header attributes 'catalogue' and 'collection'"
    id = f"{catalog} {collection} {application or ''}"
    # No return value. Results are captured by logger.
    logger.info(f"Data consistency test {id} started")
    DataConsistencyTest(catalog, collection, application).run()
    logger.info(f"Data consistency test {id} ended")

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
