#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base SQL Alchemy ORM entity"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, Date, Index, Integer, String, Text
from sqlalchemy.ext.declarative import (AbstractConcreteBase, declarative_base,
                                        declared_attr)

BASE = declarative_base()


class BaseEntity(AbstractConcreteBase, BASE):
    __tablename__ = None
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)
    # Catalog identifier, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    # Full name
    name = Column(Text, nullable=False)
    # Tokenized full name, can be null. See text_utils#tokenize
    name_tokens = Column(Text)
    # Date of birth
    born = Column(Date)
    # Date of birth precision
    born_precision = Column(Integer)
    # Date of death
    died = Column(Date)
    # Date of death precision
    died_precision = Column(Integer)

    # Full-text index over 'name' and 'name_tokens'
    @declared_attr
    def __table_args__(cls):
        return (
            Index('ftix_name_tokens_%s' % cls.__tablename__,
                  "name_tokens", mysql_prefix="FULLTEXT"),
            {'mysql_charset': 'utf8mb4'}
        )

    def __repr__(self) -> str:
        return "<BaseEntity(catalog_id='{0}', name='{1}')>".format(self.catalog_id, self.name)


class BaseRelationship(AbstractConcreteBase, BASE):
    __tablename__ = None
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    from_catalog_id = Column(String(50), nullable=False, index=False)
    to_catalog_id = Column(String(50), nullable=False, index=False)

    # Regular double index
    @declared_attr
    def __table_args__(cls):
        return (
            Index('idx_catalog_ids_%s' % cls.__tablename__, 'from_catalog_id',
                  'to_catalog_id', unique=True, mysql_using='hash'),
            {'mysql_charset': 'utf8mb4'}
        )

    def __init__(self, from_catalog_id: str, to_catalog_id: str):
        self.from_catalog_id = from_catalog_id
        self.to_catalog_id = to_catalog_id

    def __repr__(self):
        return '<BaseRelationship object({0} -> {1})>'.format(self.from_catalog_id, self.to_catalog_id)
