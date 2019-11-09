# coding:utf-8
import logging

from dht_spider.logger.colorlog import ColoredFormatter


def setup_logger(name='Logger', level=logging.INFO):
    """Return a logger with a default ColoredFormatter."""
    loggers = logging.getLogger(name)
    if len(loggers.handlers) < 1:
        formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s%(white)s %(filename)s %(lineno)d : %(message)s",
            # "%(log_color)s%(asctime)s%(white)s : %(message)s",
            datefmt='%Y/%m/%d %H:%M:%S',
            reset=True,
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        # logger.handlers = []
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        loggers.addHandler(handler)
        loggers.setLevel(level)
        loggers.propagate = False
    return loggers


logger = setup_logger()

if __name__ == '__main__':
    logger = setup_logger()
    logger.debug('a debug message')
    logger.info('an info message')
    logger.warning('a warning message')
    logger.error('an error message')
    logger.critical('a critical message')
