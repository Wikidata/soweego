#!/usr/bin/env python3
# coding: utf-8

import mysql.connector as mariadb
import business.utils.file_utils as file_utils
import domain.localizations as loc

class ImportService(object):
    __user = None
    __password = None 
    __database = None
    __cursor = None

    def __init__(self, credentials_path):
        configs = file_utils.load_json(credentials_path)
        self.__user = configs['user']
        self.__password = configs['password']
        self.__database = configs['database']
        mariadb_connection = mariadb.connect(user = self.__user, password = self.__password, database = self.__database)
        self.__cursor = mariadb_connection.cursor()

    def create_table(self, name, schema):
        query = 'CREATE TABLE {0}'.format(name)

        for field in schema :
            
        self.execute_query(query)

    def create_db(self):
        self.execute_query('CREATE DATABASE {0}'.format(loc.db_name))

    def execute_query(self, query):
        # TODO prevent sql injection!
        self.__cursor.execute(query)