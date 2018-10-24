#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Adapted from https://github.com/Wikidata/StrepHit/blob/master/strephit/commons/logging.py

"""Logging facility"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import logging.config
import os
from urllib.parse import unquote_plus

LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}
CONFIG_FILE_PATH = os.path.abspath(os.path.join('logging.cfg'))
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
            'class': 'logging.StreamHandler',
            'level': 'INFO'
        },
        'debug_file_handler': {
            'formatter': 'soweego',
            'level': 'DEBUG',
            'filename': 'soweego.log',
            'mode': 'w',
            'class': 'logging.FileHandler',
            'encoding': 'utf8'
        }
    }
}


def setup():
    """Set up logging via a config file if available or via the default configuration."""
    if os.path.exists(CONFIG_FILE_PATH):
        logging.config.fileConfig(CONFIG_FILE_PATH, DEFAULT_CONFIG,
                                  disable_existing_loggers=False)
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
