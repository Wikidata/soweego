#!/usr/bin/env python3
# coding: utf-8

from sqlalchemy import create_engine
import logging

from .. import constants as const
from .. import localizations as loc
from ..utils import file_utils

LOGGER = logging.getLogger(__name__)

class DBManager(object):

    __engine: object

    def __init__(self, credentials_path):
        # Base = declarative_base()
        try:
            credentials = file_utils.load_json(const.db_credentials)
            environment = file_utils.load_json(const.cofigs_path)[const.environment_key]
        except:
            LOGGER.warning(loc.missing_credentials)
            raise Exception(loc.missing_credentials)

        db_engine = credentials[const.db_engine_key] 
        db_name = credentials[const.prod_db_key]
        user = credentials[const.user_key]
        password = credentials[const.password_key]
        host = credentials[const.host_key]

        if environment == const.development:
            db_name = credentials[const.test_db_key]   

        self.__engine = create_engine(
            '{0}://{1}:{2}@{3}/{4}'.format(db_engine, user, password, host, db_name), 
            echo=True)
