#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

import os
import re
import datetime
import logging
from sqlalchemy import Column
from sqlalchemy import String

from soweego.importer.services.db_manager import DBManager
from soweego.importer.models.dump_state import DumpState
from soweego.importer.models.dump_state import Mappings
from soweego.importer.models.orm.base_entity import BaseEntity

LOGGER = logging.getLogger(__name__)


def handle(file_path: str, mappings: dict, orm_model: BaseEntity):
    """TODO docstring"""
    db_manager = DBManager('C:\\Code\Wikidata.Soweego\\soweego\\importer\\resources\\db_credentials.json') #TODO relative/OS independent path
    session = db_manager.new_session()

    if not os.path.isfile(file_path):
        LOGGER.warning("file: %s not found", file_path)

    with open(file_path) as file:
        rows = file.readlines() 

        LOGGER.info('%s \t Start import', datetime.datetime.now())
        #id_regex_chunks = re.escape(dump_state.mappings[0].source_name).split('\\$1') #TODO
        #id_regex = '(?<=' + id_regex_chunks[0] + ').*(?=' + id_regex_chunks[1] + ')'
        #(?<=\<http:\/\/data.bibsys.no\/data\/notrbib\/authorityentry\/).*(?=\>)

        current_key = None
        current_entity = orm_model()

        for row in rows:
            row_chunks = row.split(' ')
            try:

                current_field = mappings[row_chunks[1]]

                if(current_key != row_chunks[0]):
                    session.add(current_entity)
                    current_entity = BaseEntity()

                current_key = row_chunks[0]
                current_value = row_chunks[2]

                setattr(current_entity, 'key', current_key)
                setattr(current_entity, current_field, current_value)
                
            except Exception as exception: 
                LOGGER.warning('Error at row')

def save_entity(entity, mappings: Mappings):
    pass

def decode_name(name: str) -> str:
    return name.encode('ascii').decode('unicode-escape') 


#def ClassFactory(name, argnames, BaseClass=Base):
#    def __init__(self, **kwargs):
#        for key, value in kwargs.items():
#            setattr(self, key, value)
#        BaseClass.__init__(self, name[:-len("Class")])
#    newclass = type(name, (BaseClass,),{"__init__": __init__})
#    return newclass