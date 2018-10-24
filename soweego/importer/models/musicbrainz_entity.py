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


class MusicBrainzEntity(BaseEntity, BASE):
    __tablename__ = "musicbrainz"

    # TODO define missing non-standard fields


class MusicBrainzLinkEntity(BaseLinkEntity, BASE):
    __tablename__ = "musicbrainz_link"

    catalog_id = Column(String(32), ForeignKey(MusicBrainzEntity.catalog_id),
                        index=True)
