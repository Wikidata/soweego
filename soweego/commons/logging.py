#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Adapted from https://github.com/Wikidata/StrepHit/blob/master/strephit/commons/logging.py

"""Logging facility"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
import logging.config
import os
from io import StringIO
from urllib.parse import unquote_plus

import tqdm

LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}
CONFIG_FILE_PATH = os.path.abspath(os.path.join('logging.json'))
DEFAULT_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        '': {
            'level': 'WARNING',
            'handlers': ['console', 'debug_file_handler']
        },
        'soweego': {
            'level': 'INFO',
        },
    },
    'formatters': {
        'soweego': {
            'format': '%(asctime)s [%(levelname)s] %(module)s.%(funcName)s #%(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'formatter': 'soweego',
            'class': 'soweego.commons.logging.TqdmLoggingHandler',
            'level': 'INFO'
        },
        'debug_file_handler': {
            'formatter': 'soweego',
            'level': 'DEBUG',
            'filename': 'debug.log',
            'mode': 'w',
            'class': 'logging.FileHandler',
            'encoding': 'utf8'
        }
    }
}


class TqdmLoggingHandler (logging.StreamHandler):
    """
    Custom logging handler. This ensures that TQDM
    progress bars always stay at the bottom of the
    terminal instead of being printed as normal
    messages
    """

    def __init__(self, stream=None):
        super().__init__(stream)

    # we only overwrite `Logging.StreamHandler`
    # emit method. Everything else is
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def setup():
    """Set up logging via a config file if available or via the default configuration."""
    if os.path.exists(CONFIG_FILE_PATH):
        logging.config.dictConfig(json.load(open(CONFIG_FILE_PATH)))
    else:
        logging.config.dictConfig(DEFAULT_CONFIG)


def set_log_level(module, level):
    """Set the log level used to log messages from the given module."""
    if level in LEVELS:
        module = '' if module == 'root' else module
        logging.getLogger(module).setLevel(level)


def log_request_data(http_response, logger):
    """Send a debug log message with basic information
    of the HTTP request that was sent for the given HTTP response.

    :param requests.models.Response http_response: HTTP response object
    """
    sent_request = {
        'method': http_response.request.method,
        'url': http_response.request.url,
        'headers': http_response.request.headers,
        'decoded_body': unquote_plus(repr(http_response.request.body))
    }
    logger.debug("Request sent: %s", sent_request)
    return 0


def log_dataframe_info(logger, dataframe, message):
    debug_buffer = StringIO()
    dataframe.info(buf=debug_buffer)
    logger.debug(message + ': %s', debug_buffer.getvalue())
