#!/usr/bin/env python3
# coding: utf-8

from sqlalchemy import create_engine
import logging

from .. import constants as const
from .. import localizations as loc
from ..utils import json_utils

LOGGER = logging.getLogger(__name__)

class DBManager(object):

    __engine: object

    def __init__(self, credentials_path):
        # Base = declarative_base()
        try:
            credentials = json_utils.load(const.DB_CREDENTIALS)
            environment = json_utils.load(const.CONFIGS_PATH)[const.ENVIRONMENT_KEY]
        except:
            LOGGER.warning(loc.MISSING_CREDENTIALS)
            raise Exception(loc.MISSING_CREDENTIALS)

        db_engine = credentials[const.DB_ENGINE_KEY] 
        db_name = credentials[const.PROD_DB_KEY]
        user = credentials[const.USER_KEY]
        password = credentials[const.PASSWORD_KEY]
        host = credentials[const.HOST_KEY]

        if environment == const.DEVELOPMENT:
            db_name = credentials[const.TEST_DB_KEY]   

        self.__engine = create_engine(
            '{0}://{1}:{2}@{3}/{4}'.format(db_engine, user, password, host, db_name), 
            echo=True)
