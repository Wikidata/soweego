#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entity for
textual data that will undergo some natural language processing (*NLP*)."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from sqlalchemy import Column, Index, Integer, String, Text
from sqlalchemy.ext.declarative import (
    AbstractConcreteBase,
    declarative_base,
    declared_attr,
)

BASE = declarative_base()


class BaseNlpEntity(AbstractConcreteBase, BASE):
    """Minimal ORM structure for a target catalog piece of text.
    Each ORM NLP entity should implement this interface.

    **Attributes:**

    - **internal_id** (integer) - an internal primary key
    - **catalog_id** (string(50)) - a target catalog identifier
    - **description** (text) - a text describing the main catalog entry
    - **description_tokens** (text) - a **description** tokenized through
      :func:`~soweego.commons.text_utils.tokenize`

    """

    __tablename__ = None
    internal_id = Column(
        Integer, unique=True, primary_key=True, autoincrement=True
    )
    # Catalog identifier of the entity with textual data, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    # Original text
    description = Column(Text)
    # Tokenized by us
    description_tokens = Column(Text)

    # Full-text index over 'description'
    @declared_attr
    def __table_args__(cls):
        return (
            Index(
                'ftix_description_%s' % cls.__tablename__,
                "description",
                mysql_prefix="FULLTEXT",
            ),
            {'mysql_charset': 'utf8mb4'},
        )

    def __repr__(self) -> str:
        return "<BaseNlpEntity(catalog_id='{0}', description='{1}')>".format(
            self.catalog_id, self.description
        )
