import logging
import sys

from celery.signals import after_setup_logger


logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")

console_handler = logging.StreamHandler(sys.stdout)

console_handler.setFormatter(formatter)

logger.handlers = [console_handler]
