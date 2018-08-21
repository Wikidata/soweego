#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring
see https://mariadb.com/kb/en/library/full-text-index-overview/
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging

import click
from pymysql import OperationalError
from pymysql.cursors import DictCursor
from toolforge import connect

LOGGER = logging.getLogger(__name__)
TEST_DB = 's53821__test_index'
PROD_DB = 's51434__mixnmatch_large_catalogs'
HOST = 'tools.db.svc.eqiad.wmflabs'
INDEX_TABLE = 'names_index'
INDEX_COLUMN = 'name'
CREATE_INDEX_COMMAND = 'CREATE TABLE %s(%s TEXT,FULLTEXT(%s)) ENGINE=InnoDB;' % (
    INDEX_TABLE, INDEX_COLUMN, INDEX_COLUMN)
INSERT_VALUES_COMMAND = 'INSERT INTO %s(%s) VALUES ' % (
    INDEX_TABLE, INDEX_COLUMN)
DROP_INDEX_COMMAND = 'DROP TABLE %s' % INDEX_TABLE
QUERY_COMMAND = "SELECT %s,MATCH(%s) AGAINST('{}'{}) AS relevance FROM %s WHERE MATCH(%s) AGAINST('{}'{});" % (
    INDEX_COLUMN, INDEX_COLUMN, INDEX_TABLE, INDEX_COLUMN)


@click.command()
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
def drop_index(database):
    """Drop an index table on a MariaDB user database in Toolforge."""
    connection = _create_connection(database)
    if not connection:
        return
    try:
        with connection.cursor() as cursor:
            cursor.execute(DROP_INDEX_COMMAND)
        connection.commit()
    finally:
        connection.close()
    LOGGER.info("Dropped index table '%s' on database '%s' at %s",
                INDEX_TABLE, connection.db, connection.host)


@click.command()
@click.argument('dataset_file', type=click.File())
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
def build_index(dataset_file, database):
    """Build an index table on a MariaDB user database in Toolforge.

    The index can be later used to gather match candidates for a Wikidata entity
    from a target catalog.
    See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases
    for more details on Toolforge user databases.
    """
    connection = _create_connection(database)
    if not connection:
        return
    dataset = json.load(dataset_file)
    # Default index analyzer behavior:
    # https://mariadb.com/kb/en/library/full-text-index-overview/#excluded-results
    # https://mariadb.com/kb/en/library/xtradbinnodb-server-system-variables/#innodb_ft_min_token_size
    # https://mariadb.com/kb/en/library/xtradbinnodb-server-system-variables/#innodb_ft_max_token_size
    # https://mariadb.com/kb/en/library/full-text-index-stopwords/#innodb-stopwords
    # TODO fuzzy analyzer
    insert_command = INSERT_VALUES_COMMAND
    for name in dataset.keys():
        insert_command += '("%s"), ' % name.replace('(',
                                                    '\\(').replace(')', '\\)').replace('"', '\\"')
    insert_command = insert_command.rstrip(', ') + ';'
    try:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_INDEX_COMMAND)
            cursor.execute(insert_command)
        connection.commit()
    finally:
        connection.close()
    LOGGER.info("Created index column '%s' with %s values in table '%s' on database '%s' at %s",
                INDEX_COLUMN, len(dataset), INDEX_TABLE, connection.db, connection.host)


def _create_connection(database_name):
    try:
        connection = connect(database_name, host=HOST, cursorclass=DictCursor)
    except OperationalError as op_error:
        LOGGER.error(op_error)
        return None
    LOGGER.info('Connected to database "%s" at %s',
                connection.db, connection.host)
    return connection


@click.command()
@click.argument('query')
@click.option('-s', '--search-type', type=click.Choice(
    ['natural_language', 'boolean', 'expansion']), default='natural_language')
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
def query_index(query, search_type, database):
    """Query the index table located on a MariaDB user database in Toolforge.

    The index contains a set of target catalog entities and can be queried
    to gather match candidates for a Wikidata entity.

    See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases
    for more details on Toolforge user databases.
    """
    connection = _create_connection(database)
    if not connection:
        return
    if search_type == 'natural_language':
        command = QUERY_COMMAND.format(query, '', query, '')
    elif search_type == 'boolean':
        boolean_mode = ' IN BOOLEAN MODE'
        command = QUERY_COMMAND.format(
            query, boolean_mode, query, boolean_mode)
    elif search_type == 'expansion':
        expansion_mode = ' WITH QUERY EXPANSION'
        command = QUERY_COMMAND.format(
            query, expansion_mode, query, expansion_mode)
    LOGGER.debug("About to run query: %s", command)
    try:
        with connection.cursor() as cursor:
            result_count = cursor.execute(command)
            results = cursor.fetchall()
    finally:
        connection.close()
    LOGGER.info('Got %s results: %s', result_count,
                json.dumps(results, indent=2, ensure_ascii=False))
    return results
