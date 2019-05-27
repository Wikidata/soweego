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

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity

BASE = declarative_base()
BASE_ENTITY = 'discogs_base_entity'
MUSICIAN_TABLE = 'discogs_musician'
MUSICIAN_LINK_TABLE = 'discogs_musician_link'
MUSICIAN_NLP_TABLE = 'discogs_musician_nlp'
GROUP_TABLE = 'discogs_group'
GROUP_LINK_TABLE = 'discogs_group_link'
GROUP_NLP_TABLE = 'discogs_group_nlp'
MASTER_TABLE = 'discogs_master'
MASTER_ARTIST_RELATIONSHIP_TABLE = 'discogs_master_artist_relationship'


class DiscogsBaseEntity(BaseEntity):
    __tablename__ = BASE_ENTITY
    # Name in real life
    real_name = Column(Text)
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


class DiscogsMasterEntity(DiscogsBaseEntity):
    __tablename__ = MASTER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    main_release_id = Column(String(50))
    genres = Column(Text)
    data_quality = Column(String(50))


class DiscogsMasterArtistRelationship(BaseRelationship):
    __tablename__ = MASTER_ARTIST_RELATIONSHIP_TABLE

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    def __repr__(self):
        return super().__repr__()
