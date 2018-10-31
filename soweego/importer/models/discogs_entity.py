#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs SQL Alchemy ORM model for musicians"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity

BASE = declarative_base()
MUSICIAN_TABLE = 'discogs_musician'
MUSICIAN_LINK_TABLE = 'discogs_musician_link'
MUSICIAN_NLP_TABLE = 'discogs_musician_nlp'
GROUP_TABLE = 'discogs_group'
GROUP_LINK_TABLE = 'discogs_group_link'
GROUP_NLP_TABLE = 'discogs_group_nlp'


class DiscogsBaseEntity(BaseEntity):
    # Name in real life
    real_name = Column(String(255))
    # Discogs-specific indicator of data quality
    data_quality = Column(String(20))


class DiscogsMusicianEntity(DiscogsBaseEntity, BASE):
    __tablename__ = MUSICIAN_TABLE


class DiscogsMusicianLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = MUSICIAN_LINK_TABLE


class DiscogsMusicianNlpEntity(BaseNlpEntity, BASE):
    __tablename__ = MUSICIAN_NLP_TABLE


class DiscogsGroupEntity(DiscogsBaseEntity, BASE):
    __tablename__ = GROUP_TABLE


class DiscogsGroupLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = GROUP_LINK_TABLE


class DiscogsGroupNlpEntity(BaseNlpEntity, BASE):
    __tablename__ = GROUP_NLP_TABLE
