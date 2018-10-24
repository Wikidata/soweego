#!/usr/bin/env python3
# coding: utf-8

"""DB manager based on sqlalchemy engine"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import logging

from soweego.commons import constants as const
from soweego.commons import json_utils
from soweego.commons import localizations as loc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DECLARATIVE_BASE = declarative_base()
LOGGER = logging.getLogger(__name__)


class DBManager(object):

    """Class that exposes some primitives for the DB access"""

    __engine: object

    def __init__(self, credentials):

        db_engine = credentials[const.DB_ENGINE_KEY]
        db_name = credentials[const.PROD_DB_KEY]
        user = credentials[const.USER_KEY]
        password = credentials[const.PASSWORD_KEY]
        host = credentials[const.HOST_KEY]
        try:
            self.__engine = create_engine(
                '{0}://{1}:{2}@{3}/{4}'.format(db_engine,
                                               user, password, host, db_name),
                echo=False)
        except:
            LOGGER.warning(loc.FAIL_CREATE_ENGINE)

    def get_engine(self) -> Engine:
        """returns the current SqlAlchemy-Engine instance"""
        return self.__engine

    def new_session(self) -> object:
        """Creates a new DB session"""
        Session = sessionmaker(bind=self.__engine)
        session = Session()
        return session

    def create(self, table) -> None:
        """Creates the schema of the table (table can be an instance or a type)"""
        DECLARATIVE_BASE.metadata.create_all(self.__engine,
                                             tables=[table.__table__])

    def drop(self, table) -> None:
        """Drops the table (table can be an instance or a type)"""
        DECLARATIVE_BASE.metadata.drop_all(self.__engine,
                                           tables=[table.__table__])
