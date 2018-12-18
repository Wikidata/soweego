#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base SQL Alchemy ORM entity"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, Date, Index, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import (AbstractConcreteBase, declarative_base,
                                        declared_attr)

BASE = declarative_base()


class BaseEntity(AbstractConcreteBase, BASE):
    __tablename__ = None
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)
    # Catalog identifier, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    # Full name (<name> <surname>)
    name = Column(String(255), nullable=False)
    # Full name (<name> <surname>) tokenized
    tokens = Column(String(255), nullable=False)
    # Date of birth
    born = Column(Date)
    # Date of birth precision
    born_precision = Column(Integer)
    # Date of death
    died = Column(Date)
    # Date of death precision
    died_precision = Column(Integer)

    def __repr__(self) -> str:
        return "<BaseEntity(catalog_id='{0}', name='{1}')>".format(self.catalog_id, self.name)

    @declared_attr
    def __table_args__(cls):
        return (
            Index('ftix_tokens_%s' % cls.__tablename__,
                  "tokens", mysql_prefix="FULLTEXT"),
            Index('ftix_name_%s' % cls.__tablename__,
                  "name", mysql_prefix="FULLTEXT"),
            {'mysql_charset': 'utf8mb4'}
        )


class BaseRelationship(AbstractConcreteBase, BASE):
    __tablename__ = None
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    from_catalog_id = Column(String(50), nullable=False, index=False)
    to_catalog_id = Column(String(50), nullable=False, index=False)

    @declared_attr
    def __table_args__(cls):
        return (
            Index('idx_catalog_ids_%s' % cls.__tablename__, 'from_catalog_id',
                  'to_catalog_id', unique=True),
            {'mysql_charset': 'utf8mb4'}
        )

    def __init__(self, from_id: str, to_id: str):
        self.from_catalog_id = from_id
        self.to_catalog_id = to_id

    def __repr__(self):
        return '< BaseRelationship object({} {}) >'.format(self.from_catalog_id, self.to_catalog_id)
