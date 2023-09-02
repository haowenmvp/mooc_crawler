from exceptions.offlineError import OfflineError
import logging

try:
    raise OfflineError('offline')
except Exception as e:
    logging.error(e)