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

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, sessionmaker
from sqlalchemy.pool import NullPool

from soweego.commons import constants
from soweego.commons import localizations as loc

BASE = declarative_base()
LOGGER = logging.getLogger(__name__)


# TODO this class should become a singleton
class DBManager():

    """Exposes some primitives for the DB access"""

    __engine: object
    __credentials = None

    def __init__(self):
        credentials = DBManager.get_credentials()
        db_engine = credentials[constants.DB_ENGINE]
        db_name = credentials[constants.PROD_DB]
        user = credentials[constants.USER]
        password = credentials[constants.PASSWORD]
        host = credentials[constants.HOST]
        try:
            # Disable connection pooling, as per Wikimedia policy
            # https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connection_handling_policy
            self.__engine = create_engine(
                f'{db_engine}://{user}:{password}@{host}/{db_name}', poolclass=NullPool)
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
        configure_mappers()
        BASE.metadata.create_all(self.__engine, tables=[
                                 table.__table__ for table in tables])

    def drop(self, tables) -> None:
        """Drop the tables (table can be ORM entity instances or classes)"""
        BASE.metadata.drop_all(self.__engine, tables=[
                               table.__table__ for table in tables])

    @staticmethod
    def connect_to_db():
        db_manager = DBManager()
        session = db_manager.new_session()
        return session

    @staticmethod
    def get_credentials():
        if DBManager.__credentials:
            return DBManager.__credentials
        else:
            return json.loads(
                get_data('soweego.importer.resources', 'db_credentials.json'))

    @staticmethod
    def set_credentials_from_path(path: str):
        with open(path) as file:
            DBManager.__credentials = json.load(file)
