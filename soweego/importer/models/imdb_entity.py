#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""IMDB SQL Alchemy ORM model"""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, tupini07'

from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.wikidata import vocabulary

BASE = declarative_base()

ACTOR_TABLE = 'imdb_actor'
BASE_PERSON_TABLE = 'imdb_base_person'
DIRECTOR_TABLE = 'imdb_director'
MOVIE_TABLE = 'imdb_movie'
MUSICIAN_TABLE = 'imdb_musician'
MOVIE_PERSON_RELATIONSHIP_TABLE = 'imdb_movie_person_relationship'
PRODUCER_TABLE = 'imdb_producer'
WRITER_TABLE = 'imdb_writer'


# actor, director, producer e writer


class ImdbMovieEntity(BASE):
    __tablename__ = MOVIE_TABLE
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    # Catalog identifier, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    title_type = Column(String(100))
    primary_title = Column(Text)
    original_title = Column(Text)
    is_adult = Column(Boolean)

    start_year = Column(Integer, nullable=True)
    end_year = Column(Integer, nullable=True)

    runtime_minutes = Column(Integer)
    genres = Column(String(255), nullable=True)

    def __repr__(self) -> str:
        return f'<ImdbMovieEntity(catalog_id="{self.catalog_id}", title="{self.original_title}")>'


class ImdbPersonEntity(BaseEntity):
    # each table/entity type should be associated with
    # an occupation (defined in vocabulary.py) which
    # is the main occupation for people in said table
    table_occupation = None

    __tablename__ = BASE_PERSON_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    gender = Column(String(10))

    # base imdb person entity
    born_precision = Column(Integer, default=9, nullable=False)
    died_precision = Column(Integer, default=9, nullable=False)

    # space separated string of QIDs representing an
    # occupation
    occupations = Column(String(255), nullable=True)

    __abstract__ = True


class ImdbActorEntity(ImdbPersonEntity):
    table_occupation = vocabulary.ACTOR_QID

    __tablename__ = ACTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbDirectorEntity(ImdbPersonEntity):
    table_occupation = vocabulary.FILM_DIRECTOR_QID

    __tablename__ = DIRECTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbMusicianEntity(ImdbPersonEntity):
    table_occupation = vocabulary.MUSICIAN_QID

    __tablename__ = MUSICIAN_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbProducerEntity(ImdbPersonEntity):
    table_occupation = vocabulary.FILM_PRODUCER_QID

    __tablename__ = PRODUCER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbWriterEntity(ImdbPersonEntity):
    table_occupation = vocabulary.SCREENWRITER_QID

    __tablename__ = WRITER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbMoviePersonRelationship(BaseRelationship):
    __tablename__ = MOVIE_PERSON_RELATIONSHIP_TABLE

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    def __repr__(self):
        return super().__repr__()
