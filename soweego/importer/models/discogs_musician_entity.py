#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs SQL Alchemy ORM model for musicians"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from sqlalchemy import Column, ForeignKey, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity

BASE = declarative_base()
DISCOGS_MUSICIAN_TABLE_NAME = 'discogs_musician'
DISCOGS_MUSICIAN_LINK_TABLE_NAME = 'discogs_musician_link'


class DiscogsMusicianEntity(BaseEntity, BASE):
    __tablename__ = DISCOGS_MUSICIAN_TABLE_NAME
    # Discogs identifier of a group the musician belongs to
    group_id = Column(String, ForeignKey(
        '%s.catalog_id' % DISCOGS_MUSICIAN_TABLE_NAME), index=True)  # TODO find a better way
    # Name in real life
    real_name = Column(String)
    # Other art names
    name_variations = Column(String)
    # Description
    profile = Column(Text)
    # Discogs API URL of the musician's albums
    releases_url = Column(String)
    # Discogs-specific indicator of data quality
    data_quality = Column(String)


class DiscogsMusicianLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = DISCOGS_MUSICIAN_LINK_TABLE_NAME
    catalog_id = Column(String(32), ForeignKey(DiscogsMusicianEntity.catalog_id),
                        index=True)
