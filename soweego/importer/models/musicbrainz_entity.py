#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`MusicBrainz <https://musicbrainz.org/>`_
`SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities,
based on the database
`specifications <https://musicbrainz.org/doc/MusicBrainz_Database>`_."""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, MaxFrax96'

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

ARTIST_TABLE = 'musicbrainz_artist'
ARTIST_LINK_TABLE = 'musicbrainz_artist_link'
BAND_TABLE = 'musicbrainz_band'
BAND_LINK_TABLE = 'musicbrainz_band_link'
RELEASE_GROUP_TABLE = 'musicbrainz_release_group'
RELEASE_GROUP_LINK_TABLE = 'musicbrainz_release_group_link'

ARTIST_BAND_RELATIONSHIP_TABLE = 'musicbrainz_artist_band_relationship'
RELEASE_ARTIST_RELATIONSHIP_TABLE = (
    'musicbrainz_release_group_artist_relationship'
)


class MusicBrainzArtistEntity(BaseEntity):
    """A MusicBrainz *artist*, namely a musician.

    **Attributes:**

    - **gender** (string(10)) - a gender
    - **birth_place** (string(255)) - a birth place
    - **death_place** (string(255)) - a death place

    """

    __tablename__ = ARTIST_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    gender = Column(String(10))
    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicBrainzBandEntity(BaseEntity):
    """A MusicBrainz band.

    **Attributes:**

    - **birth_place** (string(255)) - a place where the band was formed
    - **death_place** (string(255)) - a place where the band was disbanded

    """

    __tablename__ = BAND_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    birth_place = Column(String(255), nullable=True)
    death_place = Column(String(255), nullable=True)


class MusicBrainzArtistLinkEntity(BaseLinkEntity):
    """A MusicBrainz musician Web link (URL)."""

    __tablename__ = ARTIST_LINK_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class MusicBrainzBandLinkEntity(BaseLinkEntity):
    """A MusicBrainz band Web link (URL)."""

    __tablename__ = BAND_LINK_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class MusicBrainzReleaseGroupEntity(BaseEntity):
    """A MusicBrainz *release group*: a musical work,
    which is a group of *releases*.
    """

    __tablename__ = RELEASE_GROUP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class MusicBrainzReleaseGroupLinkEntity(BaseLinkEntity):
    """A MusicBrainz musical work Web link (URL)."""

    __tablename__ = RELEASE_GROUP_LINK_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


# NOTICE: catalog IDs of this entity can be both artists and bands
class MusicBrainzArtistBandRelationship(BaseRelationship):
    """A membership between a MusicBrainz artist and a MusicBrainz band."""

    __tablename__ = ARTIST_BAND_RELATIONSHIP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    def __repr__(self):
        return super().__repr__()


class MusicBrainzReleaseGroupArtistRelationship(BaseRelationship):
    """A relationship between a MusicBrainz musical work and the MusicBrainz
    musician or band who made it.
    """

    __tablename__ = RELEASE_ARTIST_RELATIONSHIP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    def __repr__(self):
        return super().__repr__()
