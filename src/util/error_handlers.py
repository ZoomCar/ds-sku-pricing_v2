from pymysql import OperationalError as pyMySqlOperationalError
from sqlalchemy.exc import OperationalError as sqlAlchemyOperationalError

from src.util.logger_provider import get_service_level_logger

logger = get_service_level_logger()


def db_error_handler(func):
    def handler(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except sqlAlchemyOperationalError as err:
            logger.error(f"sql alchemy operational error: {err}")

        except  pyMySqlOperationalError as err:
            logger.error(f"pymysql operational error: {err}")

    return handler
