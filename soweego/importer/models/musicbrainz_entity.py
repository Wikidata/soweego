#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz SQL Alchemy ORM model"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity

BASE = declarative_base()
PERSON_TABLE = 'musicbrainz_person'
PERSON_LINK_TABLE = 'musicbrainz_person_link'
PERSON_NLP_TABLE = 'musicbrainz_person_nlp'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'
BAND_NLP_TABLE = 'musicbrainz_band_nlp'


class MusicbrainzPersonEntity(BaseEntity, BASE):
    __tablename__ = PERSON_TABLE
    # TODO define missing non-standard fields


class MusicbrainzBandEntity(BaseEntity, BASE):
    __tablename__ = BAND_TABLE
    # TODO define missing non-standard fields


class MusicbrainzPersonLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = PERSON_LINK_TABLE
    catalog_id = Column(String(32), ForeignKey(MusicbrainzPersonEntity.catalog_id),
                        index=True)


class MusicbrainzBandLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = BAND_LINK_TABLE
    catalog_id = Column(String(32), ForeignKey(MusicbrainzBandEntity.catalog_id),
                        index=True)
