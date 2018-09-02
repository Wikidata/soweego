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
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.commons.models.orm.bibsys_entity import BibsysEntity

Base = declarative_base()


class LinkEntity(Base):

    def __init__(self, table_name):
        self.__tablename__ = table_name

    __tablename__ = 'bibsys_linker'

    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)
    catalog_id = Column(String(32), ForeignKey(BibsysEntity.catalog_id), index=True)
    # Full URL
    url = Column(String(255))
    # Tokenized URL
    tokens = Column(String(255))
    # Full-text index over the 'tokens' column
    Index('tokens_index', tokens, mysql_prefix='FULLTEXT')

    def __repr__(self) -> str:
        return "<LinkEntity(catalog_id='{0}', url='{1}')>".format(self.catalog_id, self.url)

    def drop(self, engine: Engine) -> None:
        self.__table__.drop(engine)

    def create(self, engine: Engine) -> None:
        Base.metadata.create_all(engine)