#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`Discogs <https://www.discogs.com/>`_
`SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

from sqlalchemy import Column, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity

BASE = declarative_base()

BASE_TABLE = 'discogs_base'  # Abstract table

ARTIST_TABLE = 'discogs_artist'
MASTER_TABLE = 'discogs_master'
MUSICIAN_TABLE = 'discogs_musician'
MUSICIAN_LINK_TABLE = 'discogs_musician_link'
MUSICIAN_NLP_TABLE = 'discogs_musician_nlp'
GROUP_TABLE = 'discogs_group'
GROUP_LINK_TABLE = 'discogs_group_link'
GROUP_NLP_TABLE = 'discogs_group_nlp'

MASTER_ARTIST_RELATIONSHIP_TABLE = 'discogs_master_artist_relationship'


# TODO rename to DiscogsBaseEntity
class DiscogsArtistEntity(BaseEntity):
    """A Discogs *artist*: either a musician or a band.
    It comes from the ``_artists.xml.gz`` dataset.
    See the `download page <https://data.discogs.com/>`_.

    All ORM entities describing Discogs people should inherit this class.

    .. note::
        the heuristic to distinguish between a musician and a band
        is based on the ``<groups>`` and  ``<members>`` XML tags respectively.
        In case of no such tags, you should use :class:`DiscogsArtistEntity`
        to represent a generic artist.

    **Attributes:**

    - **real_name** (text) - a name in real life
    - **data_quality** (string(20)) - an indicator of data quality

    """

    __tablename__ = BASE_TABLE

    # Name in real life
    real_name = Column(Text)

    # Discogs-specific indicator of data quality
    data_quality = Column(String(20))

    __abstract__ = True


class DiscogsMasterEntity(BaseEntity):
    """A Discogs *master*: a musical work, which can have multiple *releases*.
    It comes from the ``_masters.xml.gz`` dataset.
    See the `download page <https://data.discogs.com/>`_.

    **Attributes:**

    - **main_release_id** (string(50)) - a Discogs identifier of the
      main release for this musical work
    - **genres** (text) - a string list of musical genres

    """

    __tablename__ = MASTER_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    main_release_id = Column(String(50))
    genres = Column(Text)

    # Discogs-specific indicator of data quality
    data_quality = Column(String(20))


# TODO rename to DiscogsArtistEntity
class DiscogsGenericEntity(DiscogsArtistEntity):
    """A Discogs generic artist."""

    __tablename__ = ARTIST_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsMusicianEntity(DiscogsArtistEntity):
    """A Discogs musician."""

    __tablename__ = MUSICIAN_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsMusicianLinkEntity(BaseLinkEntity):
    """A Discogs musician Web link (URL)."""

    __tablename__ = MUSICIAN_LINK_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsMusicianNlpEntity(BaseNlpEntity, BASE):
    """A Discogs musician textual description."""

    __tablename__ = MUSICIAN_NLP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsGroupEntity(DiscogsArtistEntity):
    """A Discogs *group*, namely a band."""

    __tablename__ = GROUP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsGroupLinkEntity(BaseLinkEntity):
    """A Discogs band Web link (URL)."""

    __tablename__ = GROUP_LINK_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsGroupNlpEntity(BaseNlpEntity, BASE):
    """A Discogs band textual description."""

    __tablename__ = GROUP_NLP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class DiscogsMasterArtistRelationship(BaseRelationship):
    """A relationship between a Discogs musical work and the Discogs
    musician or band who made it."""

    __tablename__ = MASTER_ARTIST_RELATIONSHIP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    def __repr__(self):
        return super().__repr__()
