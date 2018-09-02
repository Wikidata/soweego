#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

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
from soweego.importer.commons.models.orm.base_entity import BaseEntity
from soweego.importer.commons.models.orm.link_entity import LinkEntity

LOGGER = logging.getLogger(__name__)


def handle(file_path: str, mappings: dict, orm_model):
    """Generic .nt file dump handler for the dump import into the DB.
    Assumptions: each entity must be represented in a compact block of ''adjacent'' lines
    """
    db_manager = DBManager(get_path('soweego.importer.resources', 'db_credentials.json'))
    session = db_manager.new_session()

    if not os.path.isfile(file_path):
        LOGGER.warning("file: %s not found", file_path)

    with open(file_path) as file:
        current_key = None
        current_entity = orm_model()
        link_field = list(mappings.keys())[list(mappings.values()).index("url")]

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

                if current_field == link_field:
                    link = LinkEntity("bibsys_linker") #TODO
                    link.url = row_chunks[2]
                    link.tokens = row_chunks[2].replace("/", " ") #TODO
                    link.catalog_id = current_entity.catalog_id
                    session.add(link)

                if current_key is not None and current_key != row_chunks[0]:
                    session.add(current_entity)
                    current_entity = orm_model()

                current_key = row_chunks[0]
                current_value = row_chunks[2]

                setattr(current_entity, current_field, current_value)
                
            except Exception as e: 
                LOGGER.warning('Error at row %s \n %s', row, str(e))
        session.add(current_entity)
    try:
        current_entity.create(db_manager.get_engine())
        LinkEntity("bibsys_linker").create(db_manager.get_engine())
        session.commit()
    except Exception as e: 
        LOGGER.warning(loc.WRONG_MAPPINGS % str(e))
