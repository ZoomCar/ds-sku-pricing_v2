import logging
import logging.handlers
import os
import pickle

from src.util.file_util import check_and_create_folder
from src.util.path_navigator import PathNavigator


def get_service_level_logger(_service_name=None):
    """
    Args:
        service_name:
    """
    service_name = _service_name or "pricing_service"
    log_dir_path = PathNavigator.log_folder_path()
    service_folder_path = os.path.join(log_dir_path, service_name)
    check_and_create_folder(service_folder_path)
    log_file_name = service_name + ".log"
    log_file_path = os.path.join(service_folder_path, log_file_name)

    logger_pickle_storage_path = os.path.join(log_dir_path, service_name)
    check_and_create_folder(logger_pickle_storage_path)
    pickle_file_name = str(service_name) + ".pkl"
    pickle_file_path = os.path.join(logger_pickle_storage_path, pickle_file_name)

    should_dump = False
    if os.path.exists(pickle_file_path):
        try:
            with open(pickle_file_path, "rb") as f:
                logger = pickle.load(f)
        except Exception as e:
            logger = logging.getLogger(service_name)

    else:
        should_dump = True
        logger = logging.getLogger(service_name)

    log_format = logging.Formatter('%(asctime)-2s %(process)d %(thread)d %(levelname)-2s %(filename)-2s %('
                                   'funcName)-2s %(lineno)-2s %(message)s')

    if logger.handlers:
        logger.handlers = []

    c_handler = logging.StreamHandler()
    f_handler = logging.handlers.RotatingFileHandler(log_file_path, mode='a', maxBytes=1000000000, backupCount=10)

    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.DEBUG)

    c_handler.setFormatter(log_format)
    f_handler.setFormatter(log_format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    # set level to DEBUG (important, default is set as warning, it will suppress info and debug levels)
    logger.setLevel(logging.DEBUG)

    # dump only for the first time
    if should_dump is True:
        try:
            with open(pickle_file_path, "wb") as f:
                pickle.dump(logger, f, pickle.HIGHEST_PROTOCOL)
                print("successfully dumped logger pickle for service: {0}".format(service_name))
        except Exception as e:
            pass

    return logger


def remove_logger(logger):
    # remove loggers
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)


def attach_logger(cls):
    def wrapper(*args):  # instantiating the class
        cls.logger = get_service_level_logger()  # attach logger here
        cls.auditor = []
        return cls(*args)
    return wrapper

