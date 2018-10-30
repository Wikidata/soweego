#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs SQL Alchemy ORM model for musicians"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from sqlalchemy import Column, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity

BASE = declarative_base()
DISCOGS_MUSICIAN_TABLE_NAME = 'discogs_musician'
DISCOGS_MUSICIAN_LINK_TABLE_NAME = 'discogs_musician_link'
DISCOGS_GROUP_TABLE_NAME = 'discogs_group'
DISCOGS_GROUP_LINK_TABLE_NAME = 'discogs_group_link'


class DiscogsBaseEntity(BaseEntity):
    # Name in real life
    real_name = Column(String(255))
    # Description
    profile = Column(Text)
    # Discogs-specific indicator of data quality
    data_quality = Column(String(20))


class DiscogsMusicianEntity(DiscogsBaseEntity, BASE):
    __tablename__ = DISCOGS_MUSICIAN_TABLE_NAME


class DiscogsMusicianLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = DISCOGS_MUSICIAN_LINK_TABLE_NAME


class DiscogsGroupEntity(DiscogsBaseEntity, BASE):
    __tablename__ = DISCOGS_GROUP_TABLE_NAME


class DiscogsGroupLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = DISCOGS_GROUP_LINK_TABLE_NAME
