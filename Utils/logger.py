import logging
import logging.handlers
import config

levels = {
    1: logging.DEBUG,
    2: logging.INFO,
    3: logging.WARNING,
    4: logging.ERROR,
    5: logging.CRITICAL
}


class DeviceLogger:

    @staticmethod
    def get_logger(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(f'{config.LOGS_URL}/{name}.log', maxBytes=1048576, backupCount=5)
        handler.setLevel(levels[int(config.LOG_LEVEL)])

        strfmt = '[%(asctime)s.%(msecs)03d] [%(levelname)s] > %(message)s'
        datefmt = '%Y-%m-%d,%H:%M:%S'
        formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        logger.debug(f'Logger "{name}" created')
        return logger
