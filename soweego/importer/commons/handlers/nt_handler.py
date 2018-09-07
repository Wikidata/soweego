#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import os
import re
import datetime
import logging
from sqlalchemy import Column
from sqlalchemy import String

from soweego.commons.file_utils import get_path
from soweego.commons.db_manager import DBManager
from soweego.commons import localizations as loc
from soweego.importer.commons.models.dump_state import DumpState

LOGGER = logging.getLogger(__name__)


def handle(file_path: str, mappings: dict, entity_type, link_type):
    """Generic .nt file dump handler for the dump import into the DB.
    Assumptions: each entity must be represented in a compact block of ''adjacent'' lines
    """
    db_manager = DBManager(get_path('soweego.importer.resources', 'db_credentials.json'))
    db_manager.drop(link_type)
    db_manager.drop(entity_type)
    session = db_manager.new_session()

    if not os.path.isfile(file_path):
        LOGGER.warning("file: %s not found", file_path)

    with open(file_path) as file:
        current_key = None
        current_entity = entity_type()

        for row in file.readlines():
            try:
                # split triples
                row_chunks = []
                split_regex = r'(?<=["\<])[^<>"]*(?=["\>])'
                for match in re.finditer(split_regex, row, re.I):
                    row_chunks.append(match.group(0))

                if len(row_chunks) != 3:
                    raise Exception(loc.MALFORMED_ROW)
                if row_chunks[1] in mappings:
                    current_field = mappings[row_chunks[1]]
                else:
                    raise Exception(loc.FIELD_NOT_MAPPED % row_chunks[1])

                if current_field == 'url':
                    link = link_type()
                    link.url = row_chunks[2]
                    link.tokens = row_chunks[2].replace("/", " ") #TODO
                    link.catalog_id = current_entity.catalog_id
                    session.add(link)

                if current_key is not None and current_key != row_chunks[0]:
                    session.add(current_entity)
                    current_entity = entity_type()

                current_key = row_chunks[0]
                current_value = row_chunks[2]

                setattr(current_entity, current_field, current_value)
                
            except Exception as e: 
                LOGGER.warning('Error at row %s \n %s', row, str(e))
        session.add(current_entity)
    try:
        db_manager.create(entity_type)
        db_manager.create(link_type)
        session.commit()
    except Exception as e: 
        LOGGER.warning(loc.WRONG_MAPPINGS, str(e))
