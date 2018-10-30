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


class MusicbrainzPersonEntity(BaseEntity, BASE):
    __tablename__ = "musicbrainz_person"
    # TODO define missing non-standard fields


class MusicbrainzBandEntity(BaseEntity, BASE):
    __tablename__ = "musicbrainz_band"
    # TODO define missing non-standard fields


class MusicBrainzLink(BaseLinkEntity, BASE):
    __tablename__ = "musicbrainz_link"
