import os
import logging.handlers


# create logger
def get_logger():
    logger_name = "zcs-data"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # create file handler
    logger_dir = "./log"
    if not os.path.isdir(logger_dir):
        os.makedirs(logger_dir)

    file_size_handler = logging.handlers.RotatingFileHandler(logger_dir + os.sep + logger_name + '.log',
                                                             maxBytes=104857600, backupCount=10, encoding='utf-8')
    file_size_handler.setLevel(logging.DEBUG)

    # create formatter
    fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
    datefmt = "%a %d %b %Y %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    # add handler and formatter to logger
    file_size_handler.setFormatter(formatter)
    logger.addHandler(file_size_handler)

    return logger