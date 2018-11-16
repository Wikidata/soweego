#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz SQL Alchemy ORM model"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from sqlalchemy import Column, ForeignKey, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

BASE = declarative_base()
ARTIST_TABLE = 'musicbrainz_artist'
ARTIST_LINK_TABLE = 'musicbrainz_artist_link'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'

artist_band = Table('artist_band', BASE.metadata,
                    Column('artist_id', ForeignKey(
                        '%s.internal_id' % ARTIST_TABLE), primary_key=True),
                    Column('band_id', ForeignKey(
                        '%s.internal_id' % BAND_TABLE), primary_key=True)
                    )


class MusicbrainzArtistEntity(BaseEntity, BASE):
    __tablename__ = ARTIST_TABLE

    gender = Column(String(10))
    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)

    # many to many Artist<->Band
    bands = relationship("MusicbrainzBandEntity",
                         secondary=artist_band,
                         back_populates='members',
                         lazy='dynamic')


class MusicbrainzBandEntity(BaseEntity, BASE):
    __tablename__ = BAND_TABLE

    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)

    # many to many Band<->Artist
    members = relationship("MusicbrainzArtistEntity",
                           secondary=artist_band,
                           back_populates='bands',
                           lazy='dynamic')


class MusicbrainzArtistLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = ARTIST_LINK_TABLE


class MusicbrainzBandLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = BAND_LINK_TABLE
