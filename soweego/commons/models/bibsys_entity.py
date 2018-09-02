#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Bibsys orm model"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

from sqlalchemy import Column, ForeignKey
from sqlalchemy import String

from .base_entity import BaseEntity
from .base_link_entity import BaseLinkEntity

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BibsysEntity(BaseEntity, Base):
    __tablename__ = "bibsys"

    catalogue_name = Column(String(255))
    label = Column(String(255))
    alt_label = Column(String(255))
    modified = Column(String(255))
    note = Column(String(255))
    entity_type = Column(String(255))

class BibsysLinkEntity(BaseLinkEntity, Base):
    __tablename__ = "bibsys_link"
    catalog_id = Column(String(32), ForeignKey(BibsysEntity.catalog_id), 
                        index=True)
