#!/usr/bin/env python3
# coding: utf-8

"""DB manager based on SQL Alchemy engine"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import json
import logging
from pkgutil import get_data
from typing import Iterator

from soweego.commons import constants as const
from soweego.commons import localizations as loc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

BASE = declarative_base()
LOGGER = logging.getLogger(__name__)


class DBManager():

    """Exposes some primitives for the DB access"""

    __engine: object

    def __init__(self):
        credentials = json.loads(
            get_data('soweego.importer.resources', 'db_credentials.json'))
        db_engine = credentials[const.DB_ENGINE_KEY]
        db_name = credentials[const.PROD_DB_KEY]
        user = credentials[const.USER_KEY]
        password = credentials[const.PASSWORD_KEY]
        host = credentials[const.HOST_KEY]
        try:
            # Disable connection pooling, as per Wikimedia policy
            # https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connection_handling_policy
            self.__engine = create_engine(
                '{0}://{1}:{2}@{3}/{4}'.format(db_engine, user, password, host, db_name), poolclass=NullPool)
        except Exception as error:
            LOGGER.critical(loc.FAIL_CREATE_ENGINE, error)

    def get_engine(self) -> Engine:
        """Return the current SQL Alchemy engine instance"""
        return self.__engine

    def new_session(self) -> object:
        """Create a new DB session"""
        Session = sessionmaker(bind=self.__engine)
        return Session()

    def create(self, tables) -> None:
        """Create the tables (tables can be ORM entity instances or classes)"""
        BASE.metadata.create_all(self.__engine, tables=[
                                 table.__table__ for table in tables])

    def drop(self, tables) -> None:
        """Drop the tables (table can be ORM entity instances or classes)"""
        BASE.metadata.drop_all(self.__engine, tables=[
                               table.__table__ for table in tables])

    def create_fulltext_index(self, table: str, columns: Iterator[str]) -> None:
        print(','.join(columns))
        query = "CREATE FULLTEXT INDEX ftix_%s_%s on %s(%s) ALGORITHM=INPLACE;" % (
            table, ','.join(columns), table, ','.join(columns))
        self.new_session().execute(text(query))
