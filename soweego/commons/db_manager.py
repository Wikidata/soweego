#!/usr/bin/env python3
# coding: utf-8

"""DB manager based on sqlalchemy engine"""

from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
import logging

from soweego.commons import json_utils
from soweego.commons import localizations as loc
from soweego.commons import constants as const

LOGGER = logging.getLogger(__name__)


class DBManager(object):
    
    """Class that exposes some primiteves for the DB access"""
 
    __engine: object


    def __init__(self, credentials_path='TODO default credentials path'):
        try:
            credentials = json_utils.load(credentials_path)
        except:
            LOGGER.warning(loc.MISSING_CREDENTIALS)
            raise Exception(loc.MISSING_CREDENTIALS)

        db_engine = credentials[const.DB_ENGINE_KEY] 
        db_name = credentials[const.PROD_DB_KEY]
        user = credentials[const.USER_KEY]
        password = credentials[const.PASSWORD_KEY]
        host = credentials[const.HOST_KEY]
        try:
            self.__engine = create_engine(
                '{0}://{1}:{2}@{3}/{4}'.format(db_engine, user, password, host, db_name), 
                echo=True)
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