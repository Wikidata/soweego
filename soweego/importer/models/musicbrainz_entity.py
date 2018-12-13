#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz SQL Alchemy ORM model"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity
from sqlalchemy import (Column, ForeignKey, Index, String, Table,
                        UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy_fulltext import FullText

BASE = declarative_base()
ARTIST_TABLE = 'musicbrainz_artist'
ARTIST_LINK_TABLE = 'musicbrainz_artist_link'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'
ARTIST_BAND_RELATIONSHIP_TABLE = "musicbrainz_artist_band_relationship"


class MusicbrainzArtistEntity(BaseEntity, FullText, BASE):
    __tablename__ = ARTIST_TABLE
    __fulltext_columns__ = ('tokens',)

    gender = Column(String(10))
    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicbrainzBandEntity(BaseEntity, BASE):
    __tablename__ = BAND_TABLE

    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicbrainzArtistLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = ARTIST_LINK_TABLE


class MusicbrainzBandLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = BAND_LINK_TABLE

# NOTICE: both catalog_ids of this entity can be both in Artist and Band table


class MusicBrainzArtistBandRelationship(BaseRelationship, BASE):
    __tablename__ = ARTIST_BAND_RELATIONSHIP_TABLE

    __table_args__ = (
        UniqueConstraint("from_catalog_id", "to_catalog_id"),
    )

    Index('idx_catalog_ids', 'from_catalog_id',
          'to_catalog_id', unique=True)

    def __repr__(self):
        return super().__repr__()
