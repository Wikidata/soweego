#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs SQL Alchemy ORM model for musicians"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()
BASE_ENTITY = 'discogs_base_entity'
MUSICIAN_TABLE = 'discogs_musician'
MUSICIAN_LINK_TABLE = 'discogs_musician_link'
MUSICIAN_NLP_TABLE = 'discogs_musician_nlp'
GROUP_TABLE = 'discogs_group'
GROUP_LINK_TABLE = 'discogs_group_link'
GROUP_NLP_TABLE = 'discogs_group_nlp'


class DiscogsBaseEntity(BaseEntity):
    __tablename__ = BASE_ENTITY
    # Name in real life
    real_name = Column(String(255))
    # Discogs-specific indicator of data quality
    data_quality = Column(String(20))

    __abstract__ = True


class DiscogsMusicianEntity(DiscogsBaseEntity):
    __tablename__ = MUSICIAN_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class DiscogsMusicianLinkEntity(BaseLinkEntity):
    __tablename__ = MUSICIAN_LINK_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class DiscogsMusicianNlpEntity(BaseNlpEntity, BASE):
    __tablename__ = MUSICIAN_NLP_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class DiscogsGroupEntity(DiscogsBaseEntity):
    __tablename__ = GROUP_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class DiscogsGroupLinkEntity(BaseLinkEntity):
    __tablename__ = GROUP_LINK_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class DiscogsGroupNlpEntity(BaseNlpEntity, BASE):
    __tablename__ = GROUP_NLP_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}
