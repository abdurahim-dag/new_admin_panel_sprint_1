import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
logger.addHandler(_ch)