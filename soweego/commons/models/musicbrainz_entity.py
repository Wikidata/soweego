#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""BNE orm model"""

__author__ = ''
__email__ = ''
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, '

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

from .base_entity import BaseEntity
from .base_link_entity import BaseLinkEntity

Base = declarative_base()


class MusicbrainzPersonEntity(BaseEntity, Base):
    __tablename__ = "musicbrainz_person"
    # TODO define missing non-standard fields


class MusicbrainzBandEntity(BaseEntity, Base):
    __tablename__ = "musicbrainz_band"
    # TODO define missing non-standard fields


class MusicbrainzPersonLinkEntity(BaseLinkEntity, Base):
    __tablename__ = "musicbrainz_person_link"
    catalog_id = Column(String(32), ForeignKey(MusicbrainzPersonEntity.catalog_id),
                        index=True)


class MusicbrainzBandLinkEntity(BaseLinkEntity, Base):
    __tablename__ = "musicbrainz_band_link"
    catalog_id = Column(String(32), ForeignKey(MusicbrainzBandEntity.catalog_id),
                        index=True)
