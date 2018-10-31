#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base SQL Alchemy ORM entity for textual data that will undergo NLP"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'


from sqlalchemy import Column, Index, Integer, String, Text


class BaseNlpEntity():
    __table_args__ = {'mysql_charset': 'utf8mb4'}
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)
    # Catalog identifier of the entity with textual data, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    # Original text
    description = Column(Text)
    # Tokenized by us
    tokens = Column(Text)
    # Full-text index over the original text
    Index('description_index', description, mysql_prefix='FULLTEXT')

    def __repr__(self) -> str:
        return "<BaseNlpEntity(catalog_id='{0}', description='{1}')>".format(self.catalog_id, self.description)
