#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""BNE orm model"""

__author__ = ''
__email__ = ''
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, '

from sqlalchemy import Column, ForeignKey
from sqlalchemy import String
from sqlalchemy.engine import Engine

from .base_entity import BaseEntity
from .base_link_entity import BaseLinkEntity

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DiscogsEntity(BaseEntity, Base):
    __tablename__ = "discogs"
    # TODO define missing non-standard fields

class DiscogsLinkEntity(BaseLinkEntity, Base):
    __tablename__ = "discogs_link"
    catalog_id = Column(String(32), ForeignKey(DiscogsEntity.catalog_id), 
                        index=True)
