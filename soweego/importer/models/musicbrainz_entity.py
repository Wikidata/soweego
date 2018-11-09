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
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()
ARTIST_TABLE = 'musicbrainz_artist'
ARTIST_LINK_TABLE = 'musicbrainz_artist_link'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'


class MusicbrainzArtistEntity(BaseEntity, BASE):
    __tablename__ = ARTIST_TABLE

    gender = Column(String(10))


class MusicbrainzBandEntity(BaseEntity, BASE):
    __tablename__ = BAND_TABLE
    # TODO define missing non-standard fields


class MusicbrainzArtistLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = ARTIST_LINK_TABLE


class MusicbrainzBandLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = BAND_LINK_TABLE
