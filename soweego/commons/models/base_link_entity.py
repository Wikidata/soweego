#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'


from sqlalchemy import Column, ForeignKey, Index, Integer, String
from sqlalchemy.engine import Engine

class BaseLinkEntity(object):

    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    # Full URL
    url = Column(String(255))
    # Tokenized URL
    tokens = Column(String(255))
    # Full-text index over the 'tokens' column
    Index('tokens_index', tokens, mysql_prefix='FULLTEXT')