#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`IMDb <https://www.imdb.com/>`_
`SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities, based on
the datasets `specifications <https://www.imdb.com/interfaces/>`_."""

__author__ = 'Marco Fossati, Andrea Tupini'
__email__ = 'fossati@spaziodati.eu, tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs, tupini07'

from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.wikidata import vocabulary

BASE = declarative_base()

NAME_TABLE = 'imdb_name'
TITLE_TABLE = 'imdb_title'

ACTOR_TABLE = 'imdb_actor'
DIRECTOR_TABLE = 'imdb_director'
MUSICIAN_TABLE = 'imdb_musician'
PRODUCER_TABLE = 'imdb_producer'
WRITER_TABLE = 'imdb_writer'

TITLE_NAME_RELATIONSHIP_TABLE = 'imdb_title_name_relationship'


class IMDbNameEntity(BaseEntity):
    """An IMDb *name*: a person like an actor, director, producer, etc.
    It comes from the ``name.basics.tsv.gz`` dataset.
    See the `download page <https://datasets.imdbws.com/>`_

    All ORM entities describing IMDb people should inherit this class.

    **Attributes**:

    - **gender** (string(10)) - a gender
    - **occupations** (string(255)) - a string list of Wikidata QIDs
      identifying occupations

    """

    # Each entity should be represented by its main occupation QID
    # defined in `soweego.wikidata.vocabulary`
    table_occupation = None

    __tablename__ = NAME_TABLE

    gender = Column(String(10))
    occupations = Column(String(255), nullable=True)

    # IMDb has only years, so override `BaseEntity`
    # and set default year precisions
    born_precision = Column(Integer, default=9, nullable=False)
    died_precision = Column(Integer, default=9, nullable=False)

    __abstract__ = True


class IMDbTitleEntity(BaseEntity):
    """An IMDb *title*: an audiovisual work like a movie, short,
    TV series episode, etc.
    It comes from the ``title.basics.tsv.gz`` dataset.
    See the `download page <https://datasets.imdbws.com/>`_

    All ORM entities describing IMDb works should inherit this class.

    **Attributes:**

    - **title_type** (string(100)) - an audiovisual work type, like *movie*
      or *short*
    - **primary_title** (text) - the most popular title
    - **original_title** (text) - a title in the original language
    - **is_adult** (boolean) - whether the audiovisual work is for adults or not
    - **runtime_minutes** (integer) - a runtime in minutes
    - **genres** (string(255)) - a string list of audiovisual genres

    """

    __tablename__ = TITLE_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    title_type = Column(String(100))
    primary_title = Column(Text)
    original_title = Column(Text)
    is_adult = Column(Boolean)
    runtime_minutes = Column(Integer)
    genres = Column(String(255), nullable=True)

    def __repr__(self) -> str:
        return (
            f'<IMDbTitleEntity(catalog_id="{self.catalog_id}", '
            f'original_title="{self.original_title}")>'
        )


class IMDbActorEntity(IMDbNameEntity):
    """An IMDb actor."""

    table_occupation = vocabulary.ACTOR_QID

    __tablename__ = ACTOR_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class IMDbDirectorEntity(IMDbNameEntity):
    """An IMDb director."""

    table_occupation = vocabulary.FILM_DIRECTOR_QID

    __tablename__ = DIRECTOR_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class IMDbMusicianEntity(IMDbNameEntity):
    """An IMDb musician."""

    table_occupation = vocabulary.MUSICIAN_QID

    __tablename__ = MUSICIAN_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class IMDbProducerEntity(IMDbNameEntity):
    """An IMDb producer."""

    table_occupation = vocabulary.FILM_PRODUCER_QID

    __tablename__ = PRODUCER_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class IMDbWriterEntity(IMDbNameEntity):
    """An IMDb writer."""

    table_occupation = vocabulary.SCREENWRITER_QID

    __tablename__ = WRITER_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}


class IMDbTitleNameRelationship(BaseRelationship):
    """A relationship between an IMDb audiovisual work and an IMDb
    person who took part in it."""

    __tablename__ = TITLE_NAME_RELATIONSHIP_TABLE
    __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

    def __repr__(self):
        return super().__repr__()
