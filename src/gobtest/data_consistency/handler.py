import datetime
from typing import Optional

from gobconfig.exception import GOBConfigException
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger

from gobtest.data_consistency.data_consistency_test import (
    DataConsistencyTest,
    NotImplementedApplicationError,
    NotImplementedCatalogError,
)


def can_handle(catalogue: str, collection: str, application: Optional[str] = None):
    """Is a data consistency test possible for the given cat-col-app combination.

    :param catalogue:
    :param collection:
    :param application:
    :return:
    """
    try:
        # Try to instantiate a Data Consistency Test
        DataConsistencyTest(catalogue, collection, application)
        return True
    except (GOBConfigException, NotImplementedCatalogError, NotImplementedApplicationError) as e:
        print(
            f"Data Consistency Test notification handler. Not triggering a data consistency test for {catalogue} "
            f"{collection} {application}, because not able to handle: {str(e)}"
        )


def data_consistency_test_handler(msg):
    """Request to run data consistency tests.

    :param msg:
    :return:
    """
    catalog = msg["header"].get("catalogue")
    collection = msg["header"].get("collection")
    application = msg["header"].get("application")
    msg["header"]["entity"] = msg["header"].get("entity", collection)

    assert all([catalog, collection]), "Expecting header attributes 'catalogue' and 'collection'"
    id = f"{catalog} {collection} {application or ''}"
    # No return value. Results are captured by logger.
    logger.info(f"Data consistency test {id} started")
    try:
        DataConsistencyTest(catalog, collection, application).run()
    except GOBConfigException as e:
        logger.error(f"Dataset connection failed: {str(e)}")
    except (NotImplementedCatalogError, NotImplementedApplicationError, GOBException) as e:
        logger.error(f"Dataset test failed: {str(e)}")
    else:
        logger.info(f"Data consistency test {id} ended")

    return {
        "header": {
            **msg.get("header", {}),
            "timestamp": datetime.datetime.utcnow().isoformat(),
        },
        "summary": logger.get_summary(),
    }
