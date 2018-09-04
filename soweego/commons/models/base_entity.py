#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base orm entity"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, Index
from sqlalchemy import Integer, String, Date
from sqlalchemy.engine import Engine

class BaseEntity(object):
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    # Catalog identifier, indexed
    catalog_id = Column(String(32), nullable=False, index=True)
    # Full name (<name> <surname>)
    name = Column(String(255), nullable=False)
    # Date of birth
    born = Column(Date)
    # Date of birth precision
    born_precision = Column(Integer)
    # Date of death
    died = Column(Date)
    # Date of death precision
    died_precision = Column(Integer)
    # Full-text index over the 'name' column
    Index('name_index', name, mysql_prefix='FULLTEXT')

    def __repr__(self) -> str:
        return "<BaseEntity(catalog_id='{0}', name='{1}')>".format(self.catalog_id, self.name)
