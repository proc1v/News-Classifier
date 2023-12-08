import logging
import sys

# Possible values for logging: DEBUG, INFO, WARN, ERROR and CRITICAL
LOG_LEVEL = logging.INFO  # logging level


def configure_logging():

    logging.basicConfig(format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                        datefmt='%H:%M:%S',
                        level=LOG_LEVEL,
                        handlers=[logging.StreamHandler(stream=sys.stdout)])
    return logging