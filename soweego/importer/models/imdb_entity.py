#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz SQL Alchemy ORM model"""

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
IMDB_PERSON_TABLE = "imdb_person"


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

    start_year = Column(Integer, nullable=False)
    end_year = Column(Integer, nullable=True)

    runtime_minutes = Column(Integer)
    _genres = Column("genres", String(255), nullable=True)

    @hybrid_property
    def genres(self):
        return self._genres.split(",") if self._genres else []

    @genres.setter
    def genres(self, genres):
        self._genres = ",".join(genres)

    def __repr__(self) -> str:
        return f"<ImdbMovieEntity(catalog_id='{self.catalog_id}', title='{self.original_title}')>"
