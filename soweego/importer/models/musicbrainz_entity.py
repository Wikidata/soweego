#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz SQL Alchemy ORM model"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity

BASE = declarative_base()
ARTIST_TABLE = 'musicbrainz_artist'
ARTIST_LINK_TABLE = 'musicbrainz_artist_link'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'
ARTIST_BAND_RELATIONSHIP_TABLE = "musicbrainz_artist_band_relationship"
RELEASE_GROUP_ENTITY = "musicbrainz_release_group"
RELEASE_GROUP_LINK_ENTITY = "musicbrainz_release_group_link"
RELEASE_ARTIST_RELATIONSHIP = "musicbrainz_release_group_artist_relationship"


class MusicbrainzArtistEntity(BaseEntity):
    __tablename__ = ARTIST_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    gender = Column(String(10))
    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicbrainzBandEntity(BaseEntity):
    __tablename__ = BAND_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicbrainzArtistLinkEntity(BaseLinkEntity):
    __tablename__ = ARTIST_LINK_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class MusicbrainzBandLinkEntity(BaseLinkEntity):
    __tablename__ = BAND_LINK_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class MusicbrainzReleaseGroupLinkEntity(BaseLinkEntity):
    __tablename__ = RELEASE_GROUP_LINK_ENTITY
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class MusicbrainzReleaseGroupEntity(BaseEntity):
    __tablename__ = RELEASE_GROUP_ENTITY
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


# NOTICE: both catalog_ids of this entity can be both in Artist and Band table


class MusicBrainzArtistBandRelationship(BaseRelationship):
    __tablename__ = ARTIST_BAND_RELATIONSHIP_TABLE

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    def __repr__(self):
        return super().__repr__()


# NOTICE: artist could be in artist or band table
class MusicBrainzReleaseGroupArtistRelationship(BaseRelationship):
    __tablename__ = RELEASE_ARTIST_RELATIONSHIP

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    def __repr__(self):
        return super().__repr__()
