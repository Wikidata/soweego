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
ACTOR_TABLE = "imdb_actor"
DIRECTOR_TABLE = "imdb_director"
PRODUCER_TABLE = "imdb_producer"
WRITER_TABLE = "imdb_writer"

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

    start_year = Column(Integer, nullable=False)
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


class ImdbActorEntity(BaseEntity):
    __tablename__ = ACTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbDirectorEntity(BaseEntity):
    __tablename__ = DIRECTOR_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbProducerEntity(BaseEntity):
    __tablename__ = PRODUCER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbWriterEntity(BaseEntity):
    __tablename__ = WRITER_TABLE
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}


class ImdbPersonEntity(BaseEntity):
    __tablename__ = "imdb_person"
    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'concrete': True}

    # born_precision = Column(Integer, default=9, nullable=False)
    # died_precision = Column(Integer, default=9, nullable=False)

    # birthYear – in YYYY format
    # deathYear – in YYYY format if applicable, else '\N'

    # = nconst (string) - alphanumeric unique identifier of the name/person
    # = primaryName (string)– name by which the person is most often credited

    is_actor = Column(Boolean, default=False)
    known_for_titles = Column(String(255), nullable=True)

    # primaryProfession (array of strings)– the top-3 professions of the person

    # No mapping:
    # art_department => ???? most people on wikidata have more specific occupations (art director, graphic desginer, etc)
    # camera_department =>  ?? Q1208175 Camera operator
    # casting_department =>
    # costume_department =>
    # editorial_department =>
    # electrical_department =>
    # legal =>
    # location_management =>
    # make_up_department =>
    # music_department =>
    # production_department =>
    # script_department =>
    # sound_department =>
    # soundtrack =>
    # transportation_department =>

    # Unsure:
    # animation_department => Q266569 ?animator (maker of animated films)
    # assistant => Q23835475 (nearest I could find, helper)
    # miscellaneous => ??? Maybe extra=> Q658371  <<< think about ignoring
    # visual_effects => Q1364080 VFX producer, Q1224742 digital image technician, Q28122965 effects animator

    # Ambiguous:
    # editor => Q7042855 film editor, Q23016178 tv editor (migth need to use both)

    # Direct mapping
    # actor => Q33999
    # actress => Q33999 (same ^)
    # art_director => Q706364
    # assistant_director => Q1757008
    # casting_director => Q1049296
    # cinematographer => Q222344
    # composer => Q36834
    # costume_designer => Q1323191
    # director => Q3455803
    # executive => Q978044
    # manager => Q2462658
    # producer => Q13235160
    # production_designer => Q2962070
    # production_manager => Q21292974
    # publicist => Q4178004
    # set_decorator => Q6409989
    # special_effects => Q21560152
    # stunts => Q465501
    # talent_agent => Q1344174
    # writer => Q28389
