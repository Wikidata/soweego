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

# Not really needed, just import pymysql and inspect c attributes
import toolforge

LOGGER = logging.getLogger(__name__)

# Names from target database
names = json.load(open('discogs/names_sample.json'))
# Connect to Toolforge MariaDB, soweego tool test database
# TODO switch to production database in mixnmatch tool: s51434__mixnmatch_large_catalogs_p
c = toolforge.connect('s53821__test_index', host='tools.db.svc.eqiad.wmflabs')
# MyISAM index
isam = 'CREATE TABLE myisam_index(names TEXT,FULLTEXT(names)) ENGINE=MyISAM;'
# TODO try Aria index
# Insert values
# TODO beware of default index analyzer behavior:
# https://mariadb.com/kb/en/library/full-text-index-overview/#excluded-results
# https://mariadb.com/kb/en/library/server-system-variables/#ft_min_word_len
# https://mariadb.com/kb/en/library/server-system-variables/#ft_stopword_file
# https://mariadb.com/kb/en/library/full-text-index-stopwords/
insert = "INSERT INTO myisam_index(names) VALUES "
for name in names.keys():
    insert += '("%s"), ' % name.replace('(',
                                        '\\(').replace(')', '\\)').replace('"', '\\"')
insert = insert.rstrip(', ')
insert += ';'
# See https://pymysql.readthedocs.io/en/latest/user/examples.html
try:
    with c.cursor() as cur:
        cur.execute(insert)
    c.commit()
finally:
    c.close()

# Query the index: by default, queries are NOT boolean
# TODO try boolean, query expansion
select = "SELECT names,MATCH(names) AGAINST('claudio') AS relevance FROM myisam_index WHERE MATCH(names) AGAINST('claudio');"
