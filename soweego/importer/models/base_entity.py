#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities."""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, Date, Index, Integer, String, Text
from sqlalchemy.ext.declarative import (
    AbstractConcreteBase, declarative_base, declared_attr
)

BASE = declarative_base()


class BaseEntity(AbstractConcreteBase, BASE):
    """Minimal ORM structure for a target catalog entry.
    Each ORM entity should inherit this class.

    **Attributes:**

    - **internal_id** (integer) - an internal primary key
    - **catalog_id** (string(50)) - a target catalog identifier
    - **name** (text) - a full name (person), or full title (work)
    - **name_tokens** (text) - a **name** tokenized through
      :func:`~soweego.commons.text_utils.tokenize`
    - **born** (date) - a birth (person), or publication (work) date
    - **born_precision** (integer) - a birth (person), or publication (work)
      date precision
    - **died** (date) - a death date. Only applies to a person
    - **died_precision** (integer) - a death date precision.
      Only applies to a person

    """

    __tablename__ = None

    internal_id = Column(Integer, unique=True, primary_key=True, autoincrement=True)
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

    # Full-text index over `name_tokens`
    @declared_attr
    def __table_args__(cls):
        return (
            Index(
                f'ftix_name_tokens_{cls.__tablename__}',
                'name_tokens',
                mysql_prefix='FULLTEXT',
            ),
            {'mysql_charset': 'utf8mb4'},
        )

    def __repr__(self) -> str:
        return f'<BaseEntity(catalog_id="{self.catalog_id}", ' f'name="{self.name}")>'


class BaseRelationship(AbstractConcreteBase, BASE):
    """Minimal ORM structure for a target catalog relationship
    between entries. Each ORM relationship entity should implement this
    interface.

    You can build a relationship for different purposes:
    typically, to connect works with people, or groups with individuals.

    **Attributes:**

    - **from_catalog_id** (string(50)) - a target catalog identifier
    - **to_catalog_id** (string(50)) - a target catalog identifier

    """

    __tablename__ = None
    internal_id = Column(Integer, unique=True, primary_key=True, autoincrement=True)

    from_catalog_id = Column(String(50), nullable=False, index=False)
    to_catalog_id = Column(String(50), nullable=False, index=False)

    # Regular double index
    @declared_attr
    def __table_args__(cls):
        return (
            Index(
                'idx_catalog_ids_%s' % cls.__tablename__,
                'from_catalog_id',
                'to_catalog_id',
                unique=True,
                mysql_using='hash',
            ),
            {'mysql_charset': 'utf8mb4'},
        )

    def __init__(self, from_catalog_id: str, to_catalog_id: str):
        self.from_catalog_id = from_catalog_id
        self.to_catalog_id = to_catalog_id

    def __repr__(self):
        return '<BaseRelationship object({0} -> {1})>'.format(
            self.from_catalog_id, self.to_catalog_id
        )
