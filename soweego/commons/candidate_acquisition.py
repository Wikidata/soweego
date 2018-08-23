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
from pymysql import OperationalError, ProgrammingError, escape_string
from pymysql.cursors import DictCursor
from toolforge import connect

from soweego.commons.utils import make_buckets

LOGGER = logging.getLogger(__name__)
TEST_DB = 's53821__test_index'
PROD_DB = 's51434__mixnmatch_large_catalogs'
HOST = 'tools.db.svc.eqiad.wmflabs'
INDEXED_COLUMN = 'name'
IDENTIFIER_COLUMN = 'ext_id'
# TODO create full table as per prod DB
CREATE_TABLE_COMMAND = """
CREATE TABLE %s (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ext_id` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `desc` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `born` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `died` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `gender` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `place_born` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `place_died` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `family_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `given_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `viaf` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `q` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ext_id` (`ext_id`),
  KEY `name` (`name`),
  KEY `viaf` (`viaf`),
  KEY `q` (`q`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
CREATE_INDEX_COMMAND = """
CREATE TABLE {} (
    id int(11) unsigned NOT NULL AUTO_INCREMENT,
    %s varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    %s TEXT COLLATE utf8mb4_unicode_ci NOT NULL,
    FULLTEXT(%s),
    PRIMARY KEY (id),
    KEY %s (%s)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""" % (IDENTIFIER_COLUMN, INDEXED_COLUMN, INDEXED_COLUMN, IDENTIFIER_COLUMN, IDENTIFIER_COLUMN)
INSERT_VALUES_COMMAND = 'INSERT INTO {} (%s, %s) VALUES ' % (
    INDEXED_COLUMN, IDENTIFIER_COLUMN)
DROP_INDEX_COMMAND = 'DROP TABLE IF EXISTS {}'
QUERY_COMMAND = "SELECT %s,MATCH(%s) AGAINST('{}'{}) AS relevance FROM {} WHERE MATCH(%s) AGAINST('{}'{});" % (
    INDEXED_COLUMN, INDEXED_COLUMN, INDEXED_COLUMN)


@click.command()
@click.argument('table')
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
def drop_index(table, database):
    """Drop an index table on a MariaDB user database in Toolforge."""
    connection = _create_connection(database)
    if not connection:
        return
    try:
        with connection.cursor() as cursor:
            cursor.execute(DROP_INDEX_COMMAND.format(table))
        connection.commit()
    except OperationalError as op_error:
        LOGGER.error(op_error)
        return
    finally:
        connection.close()
    LOGGER.info("Dropped index table '%s' on database '%s' at %s",
                table, connection.db, connection.host)


# TODO create text table first, then create the index
@click.command()
@click.argument('dataset_file', type=click.File())
@click.argument('target_table')
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
def build_index(dataset_file, target_table, database):
    """Build an index table on a MariaDB user database in Toolforge.

    The index can be later used to gather match candidates for a Wikidata entity
    from a target catalog.
    See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases
    for more details on Toolforge user databases.
    """
    connection = _create_connection(database)
    if not connection:
        return
    try:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_INDEX_COMMAND.format(target_table))
        connection.commit()
    except OperationalError as op_error:
        LOGGER.error(op_error)
        return
    LOGGER.info("Created table '%s' with indexed column '%s' on database '%s' at %s",
                target_table, INDEXED_COLUMN, connection.db, connection.host)
    LOGGER.info("Loading dataset '%s'", dataset_file.name)
    dataset = json.load(dataset_file)
    LOGGER.info("Dataset '%s' loaded", dataset_file.name)
    # TODO fuzzy analyzer
    # Default index analyzer behavior:
    # https://mariadb.com/kb/en/library/full-text-index-overview/#excluded-results
    # https://mariadb.com/kb/en/library/xtradbinnodb-server-system-variables/#innodb_ft_min_token_size
    # https://mariadb.com/kb/en/library/xtradbinnodb-server-system-variables/#innodb_ft_max_token_size
    # https://mariadb.com/kb/en/library/full-text-index-stopwords/#innodb-stopwords
    loaded = 0
    bucket_size = 10000
    buckets = make_buckets(list(dataset.keys()), bucket_size=bucket_size)
    LOGGER.info('Starting dataset ingestion')
    for i, bucket in enumerate(buckets):
        insert_command = INSERT_VALUES_COMMAND.format(target_table)
        for name in bucket:
            insert_command += '("%s", "%s"), ' % (escape_string(name),
                                                  escape_string(dataset[name]))
        insert_command = insert_command.rstrip(', ') + ';'
        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_command)
            connection.commit()
        except (OperationalError, ProgrammingError) as error:
            LOGGER.warning(
                'Something went wrong while adding values bucket #%d. Skipping it. Reason: %s', i, error)
            continue
        loaded += bucket_size
        LOGGER.info('%f percent ingested', (loaded/len(dataset)*100))
    connection.close()
    LOGGER.info('Dataset ingestion finished!')


def _create_connection(database_name):
    try:
        connection = connect(database_name, host=HOST, cursorclass=DictCursor)
    except OperationalError as op_error:
        LOGGER.error(op_error)
        return None
    LOGGER.debug('Connected to database "%s" at %s',
                 connection.db, connection.host)
    return connection


def query_index(query, search_type, table, database) -> dict:
    """Query the index table located on a MariaDB user database in Toolforge.

    The index contains a set of target catalog entities and can be queried
    to gather match candidates for a Wikidata entity.

    See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases
    for more details on Toolforge user databases.
    """
    connection = _create_connection(database)
    if not connection:
        return {}
    if search_type == 'natural_language':
        command = QUERY_COMMAND.format(query, '', table, query, '')
    elif search_type == 'boolean':
        boolean_mode = ' IN BOOLEAN MODE'
        command = QUERY_COMMAND.format(
            query, boolean_mode, table, query, boolean_mode)
    elif search_type == 'expansion':
        expansion_mode = ' WITH QUERY EXPANSION'
        command = QUERY_COMMAND.format(
            query, expansion_mode, table, query, expansion_mode)
    LOGGER.debug("About to run query command: %s", command)
    try:
        with connection.cursor() as cursor:
            result_count = cursor.execute(command)
            results = cursor.fetchall()
    except ProgrammingError as error:
        LOGGER.warning('Malformed query command. Reason: %s', error)
        return {}
    finally:
        connection.close()
    LOGGER.debug('Query returned %s results', result_count)
    return results


@click.command()
@click.argument('query')
@click.argument('table')
@click.option('-s', '--search-type', type=click.Choice(
    ['natural_language', 'boolean', 'expansion']), default='natural_language')
@click.option('-d', '--database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
# CLI wrapper
def run_query(query, table, search_type, database) -> dict:
    """Query the index table located on a MariaDB user database in Toolforge.

    The index contains a set of target catalog entities and can be queried
    to gather match candidates for a Wikidata entity.

    See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases
    for more details on Toolforge user databases.
    """
    return query_index(query, search_type, table, database)
