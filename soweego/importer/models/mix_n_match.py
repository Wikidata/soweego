#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`Mix'n'match <https://tools.wmflabs.org/mix-n-match/>`_
`SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities
for catalogs that need curation.

They follow the ``catalog`` and ``entry`` tables of the ``s51434__mixnmatch_p``
database located in
`ToolsDB <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases>`_
under the Wikimedia
`Toolforge <https://wikitech.wikimedia.org/wiki/Portal:Toolforge>`_
infrastructure. See how to
`connect <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connecting_to_the_database_replicas>`_.
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.dialects.mysql import INTEGER, TINYINT, TINYTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func

BASE = declarative_base()
CATALOG_TABLE = 'catalog'
ENTRY_TABLE = 'entry'

CATALOG_OWNER = 2362992  # soweego bot MnM user ID


class MnMCatalog(BASE):
    """A Mix'n'match catalog."""

    __tablename__ = CATALOG_TABLE

    id = Column(
        INTEGER(11, unsigned=True),
        unique=True,
        primary_key=True,
        autoincrement=True,
    )
    name = Column(String(128), unique=True)
    url = Column(String(128))
    desc = Column(TINYTEXT)
    type = Column(String(64), nullable=False, default='')
    wd_prop = Column(Integer, index=True)
    wd_qual = Column(Integer)
    search_wp = Column(String(16), nullable=False, default='en')
    autosync = Column(String(64), nullable=False, default='')
    active = Column(TINYINT(1), nullable=False, default=1)
    betamatch_hint = Column(String(64), nullable=False, default='')
    owner = Column(Integer, nullable=False, default=CATALOG_OWNER)
    limiter = Column(TINYTEXT, nullable=False, default='')
    note = Column(TINYTEXT, nullable=False, default='')
    source_item = Column(Integer)
    has_person_date = Column(String(16), nullable=False, default='')
    taxon_run = Column(TINYINT(4), nullable=False, default=0)


class MnMEntry(BASE):
    """A Mix'n'match entry."""

    __tablename__ = ENTRY_TABLE

    id = Column(
        INTEGER(11, unsigned=True),
        unique=True,
        primary_key=True,
        autoincrement=True,
    )
    catalog = Column(INTEGER(10, unsigned=True), nullable=False, index=True)
    ext_id = Column(String(255), nullable=False, index=True, default='')
    ext_url = Column(String(255), nullable=False, default='')
    ext_name = Column(String(128), nullable=False, index=True, default='')
    ext_desc = Column(String(255), nullable=False, default='')
    q = Column(Integer, index=True)
    user = Column(INTEGER(10, unsigned=True), index=True)
    timestamp = Column(String(16), index=True)
    random = Column(Float, index=True, default=func.rand())
    type = Column(String(16), nullable=False, index=True, default='')
