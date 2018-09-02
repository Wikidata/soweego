#!/usr/bin/env python3
# coding: utf-8

"""Bibsys-dump handler for the import into the DB"""

from soweego.commons.file_utils import get_path
from soweego.commons.json_utils import load
from soweego.importer.commons.handlers.nt_handler import handle as nt_handle
from soweego.importer.commons.models.orm.bibsys_entity import BibsysEntity

import logging
import datetime


LOGGER = logging.getLogger(__name__)


def handle(file_path: str):   
    """delegates the dump handling to the nt_handler"""
    LOGGER.info('%s \t Start bibsys import', datetime.datetime.now())
    nt_handle(file_path, 
              load(get_path('soweego.importer.bibsys.resources', 'mappings.json')), 
              BibsysEntity)
    LOGGER.info('%s \t End bibsys import', datetime.datetime.now())
