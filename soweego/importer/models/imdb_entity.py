#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""IMDB SQL Alchemy ORM model"""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, tupini07'

from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
from soweego.importer.models.base_link_entity import BaseLinkEntity
from sqlalchemy import (Column, ForeignKey, Index, String, Table,
                        UniqueConstraint, Boolean, Integer)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

BASE = declarative_base()

MOVIE_TABLE = "imdb_movie"
ACTOR_TABLE = "imdb_actor"
DIRECTOR_TABLE = "imdb_director"
PRODUCER_TABLE = "imdb_producer"
WRITER_TABLE = "imdb_writer"
BASE_PERSON_TABLE = "imdb_base_person"
PERSON_MOVIE_RELATIONSHIP_TABLE = "imdb_person_movie_relationship"

# actor, director, producer e writer


class ImdbMovieEntity(BASE):
    __tablename__ = MOVIE_TABLE
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    # Catalog identifier, indexed
    catalog_id = Column(String(50), nullable=False, index=True)
    title_type = Column(String(100))
    primary_title = Column(String(255))
    original_title = Column(String(255))
    is_adult = Column(Boolean)

    start_year = Column(Integer, nullable=True)
    end_year = Column(Integer, nullable=True)

    runtime_minutes = Column(Integer)
    _genres = Column("genres", String(255), nullable=True)

    @hybrid_property
    def genres(self):
        return self._genres.split(" ") if self._genres else []

    @genres.setter
    def genres(self, genres):
        self._genres = " ".join(genres)

    def __repr__(self) -> str:
        return f"<ImdbMovieEntity(catalog_id='{self.catalog_id}', title='{self.original_title}')>"


class ImdbPersonEntity(BaseEntity):
    __tablename__ = BASE_PERSON_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    # base imdb person entity
    born_precision = Column(Integer, default=9, nullable=False)
    died_precision = Column(Integer, default=9, nullable=False)

    _occupations = Column("occupations", String(255), nullable=True)
    @hybrid_property
    def occupations(self):
        return self._occupations.split(" ") if self._occupations else []

    @occupations.setter
    def occupations(self, occupations_):
        self._occupations = " ".join(occupations_)


    __abstract__ = True


class ImdbActorEntity(ImdbPersonEntity):
    __tablename__ = ACTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbDirectorEntity(ImdbPersonEntity):
    __tablename__ = DIRECTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbProducerEntity(ImdbPersonEntity):
    __tablename__ = PRODUCER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbWriterEntity(ImdbPersonEntity):
    __tablename__ = WRITER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbPersonMovieRelationship(BaseRelationship):
    __tablename__ = PERSON_MOVIE_RELATIONSHIP_TABLE

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    def __repr__(self):
        return super().__repr__()
