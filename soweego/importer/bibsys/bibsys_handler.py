#!/usr/bin/env python3
# coding: utf-8

"""Bibsys-dump handler for the import into the DB"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from soweego.commons.file_utils import get_path
from soweego.commons.json_utils import load
from soweego.importer.commons.handlers.nt_handler import handle as nt_handle
from soweego.commons.models.bibsys_entity import BibsysEntity, BibsysLinkEntity

import logging
import datetime


LOGGER = logging.getLogger(__name__)


def handle(file_path: str):   
    """delegates the dump handling to the nt_handler"""
    LOGGER.info('%s \t Start bibsys import', datetime.datetime.now())
    nt_handle(file_path, 
              load(get_path('soweego.importer.bibsys.resources', 'mappings.json')), 
              BibsysEntity,
              BibsysLinkEntity)
    LOGGER.info('%s \t End bibsys import', datetime.datetime.now())
