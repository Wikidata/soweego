#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs ORM model for groups"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.commons.models.base_entity import BaseEntity
from soweego.commons.models.base_link_entity import BaseLinkEntity
from soweego.commons.models.discogs_musician_entity import \
    DiscogsMusicianEntity

Base = declarative_base()

DISCOGS_GROUP_TABLE_NAME = 'discogs_group'
DISCOGS_GROUP_LINK_TABLE_NAME = 'discogs_group_link'


class DiscogsGroupEntity(BaseEntity, Base):
    __tablename__ = DISCOGS_GROUP_TABLE_NAME
    internal_id = Column(Integer, ForeignKey(BaseEntity.internal_id),
                         primary_key=True, autoincrement=True)
    # Discogs identifier of a group the group belongs to
    member_id = Column(String, ForeignKey(
        DiscogsMusicianEntity.catalog_id), index=True)
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


class DiscogsGroupLinkEntity(BaseLinkEntity, Base):
    __tablename__ = DISCOGS_GROUP_LINK_TABLE_NAME
    catalog_id = Column(String(32), ForeignKey(DiscogsGroupEntity.catalog_id),
                        index=True)
