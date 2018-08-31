#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, Date, Index, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseEntity(Base):
    __tablename__ = 'base_entity'
    internal_id = Column(Integer(11), unique=True,
                         primary_key=True, autoincrement=True)

    # Catalog identifier, indexed
    catalog_id = Column(String(32), nullable=False, index=True)
    # Full name (<name> <surname>)
    name = Column(String, nullable=False)
    # Date of birth
    born = Column(Date)
    # Date of birth precision
    born_precision = Column(Integer(2))
    # Date of death
    died = Column(Date)
    # Date of death precision
    died_precision = Column(Integer(2))
    # Full-text index over the 'name' column
    Index('name_index', name, mysql_prefix='FULLTEXT')

    def __repr__(self) -> str:
        return "<BaseEntity(catalog_id='{0}', name='{1}')>".format(self.catalog_id, self.name)

    def drop(self, engine: Engine) -> None:
        self.__table__.drop(engine)
