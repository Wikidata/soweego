#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""IMDB dump extractor"""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, tupini07'

import csv
import datetime
import gzip
import logging
import os
from typing import Dict, List

import requests

from soweego.commons import http_client as client
from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import imdb_entity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.wikidata import vocabulary as vocab

LOGGER = logging.getLogger(__name__)

DUMP_URL_PERSON_INFO = "https://datasets.imdbws.com/name.basics.tsv.gz"
DUMP_URL_MOVIE_INFO = "https://datasets.imdbws.com/title.basics.tsv.gz"


class ImdbDumpExtractor(BaseDumpExtractor):

    # Counters
    n_total_entities = 0
    n_movies = 0
    n_actors = 0
    n_directors = 0
    n_producers = 0
    n_writers = 0

    def get_dump_download_urls(self) -> List[str]:
        """
        :returns: the urls from which to download the data dumps
        the first URL is the one for the **person dump**, the
        second downloads the **movie dump**
        """
        return [DUMP_URL_PERSON_INFO, DUMP_URL_MOVIE_INFO]

    def _normalize_null(self, entity: Dict):
        """
        IMDB represents a null entry with \\N , this method converts
        all \\N to None so that they're saved as null in the database.
        This is done for all "entries" of a given entity.

        The normalization process is done in place, so this  method
        has no return value.

        :param entity: represents the entity we want to *normalize*
        """

        for k, v in entity.items():
            if v == "\\N":
                entity[k] = None

    def extract_and_populate(self, dump_file_paths: List[str]):
        """
        Extracts the data in the dumps (person and movie) and processes them.
        It then proceeds to add the appropiate data to the database. See
        :ref:`soweego.importer.models.imdb_entity` module to see the SQLAlchemy
        definition of the entities we use to save IMDB data.

        :param dump_file_paths: the absolute paths of the already downloaded dump
        files.
        """

        person_file_path = dump_file_paths[0]
        movies_file_path = dump_file_paths[1]

        LOGGER.debug("Path to person info dump: %s" % person_file_path)
        LOGGER.debug("Path to movie info dump: %s" % movies_file_path)

        start = datetime.datetime.now()

        tables = [
            imdb_entity.ImdbMovieEntity,
            imdb_entity.ImdbActorEntity,
            imdb_entity.ImdbDirectorEntity,
            imdb_entity.ImdbProducerEntity,
            imdb_entity.ImdbWriterEntity,
            imdb_entity.ImdbPersonMovieRelationship
        ]

        db_manager = DBManager()
        LOGGER.info("Connected to database: %s", db_manager.get_engine().url)

        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info("SQL tables dropped and re-created: %s",
                    [table.__tablename__ for table in tables])

        LOGGER.info("Starting to import movies from imdb dump")

        with gzip.open(movies_file_path, "rt") as mdump:
            reader = csv.DictReader(mdump, delimiter="\t")
            for movie_info in reader:
                self._normalize_null(movie_info)

                session = db_manager.new_session()

                movie_entity = imdb_entity.ImdbMovieEntity()
                movie_entity.catalog_id = movie_info.get("tconst")
                movie_entity.title_type = movie_info.get("titleType")
                movie_entity.primary_title = movie_info.get("primaryTitle")
                movie_entity.original_title = movie_info.get("originalTitle")
                movie_entity.is_adult = True if movie_info.get(
                    "isAdult") == "1" else False
                movie_entity.start_year = movie_info.get("startYear")
                movie_entity.end_year = movie_info.get("endYear")
                movie_entity.runtime_minutes = movie_info.get("runtimeMinutes")

                if movie_info.get("genres"):  # if movie has a genre specified
                    movie_entity.genres = movie_info.get("genres").split(",")

                session.add(movie_entity)
                session.commit()

                self.n_movies += 1

        LOGGER.info("Starting import persons from IMDB dump")

        with gzip.open(person_file_path, "rt") as pdump:
            reader = csv.DictReader(pdump, delimiter="\t")
            for person_info in reader:
                self._normalize_null(person_info)

                professions = person_info.get("primaryProfession")

                # if person has no professions then ignore it
                if not professions:
                    continue

                professions = professions.split(",")

                session = db_manager.new_session()

                # each person can be added to multiple tables in the DB
                types_of_entities = []

                if "actor" in professions or "actress" in professions:
                    types_of_entities.append(imdb_entity.ImdbActorEntity())

                if "director" in professions:
                    types_of_entities.append(imdb_entity.ImdbDirectorEntity())

                if "producer" in professions:
                    types_of_entities.append(imdb_entity.ImdbProducerEntity())

                if any(prof in ["sound_department", "composer",
                                "music_department", "soundtrack"]
                       for prof in professions):
                    types_of_entities.append(imdb_entity.ImdbMusicianEntity())

                if "writer" in professions:
                    types_of_entities.append(imdb_entity.ImdbWriterEntity())

                # if the only profession a person has is `miscellaneous` then we
                # add it to all 4 tables
                if professions == "miscellaneous":
                    types_of_entities = [
                        imdb_entity.ImdbActorEntity(),
                        imdb_entity.ImdbDirectorEntity(),
                        imdb_entity.ImdbMusicianEntity(),
                        imdb_entity.ImdbProducerEntity(),
                        imdb_entity.ImdbWriterEntity(),
                    ]

                # add person to every matching table
                for etype in types_of_entities:
                    self._populate_person(etype, person_info, session)

                # if person is known for any movies then add these to the
                # databse as well
                if person_info.get("knownForTitles"):
                    self._populate_person_movie_relations(person_info, session)

                session.commit()

    def _translate_professions(self, professions: List[str]) -> List[str]:
        """
        Gets the list of professions (as a list of strings) directly from IMDB
        and translates these to a list of QIDs for each specific profession.

        Unmappable professions (like `miscellaneous` are removed)

        :param professions: list of profession names, given by IMDB

        :returns: list of QIDs for said professions
        """
        qids = []

        # TODO: finish mapping
        mappings = {
            "actor": vocab.ACTOR,
            "actress": vocab.ACTOR,
            "art_director": vocab.ART_DIRECTOR,
            "assistant_director": vocab.ASSISTANT_DIRECTOR,
            "animation_department": vocab.ANIMATOR,
            "camera_department": vocab.CAMERA_OPERATOR,
            "casting_director": vocab.CASTING_DIRECTOR,
            "cinematographer": vocab.CINEMATOGRAPHER,
            "composer": vocab.COMPOSER,
            "costume_designer": vocab.COSTUME_DESIGNER,
            "director": vocab.DIRECTOR,
            "editor": vocab.FILM_EDITOR,
            "executive": vocab.EXECUTIVE,
            "manager": vocab.MANAGER,
            "music_department": vocab.MUSICIAN,
            "producer": vocab.FILM_PRODUCER,
            "production_designer": vocab.PRODUCTION_DESIGNER,
            "production_manager": vocab.PRODUCTION_MANAGER,
            "publicist": vocab.PUBLICIST,
            "set_decorator": vocab.SET_DECORATOR,
            "sound_department": vocab.SOUND_DEPARTMENT,
            "soundtrack": vocab.MUSICIAN,
            "special_effects": vocab.SPECIAL_EFFECTS,
            "stunts": vocab.STUNTS,
            "talent_agent": vocab.TALENT_AGENT,
            "writer": vocab.SCREENWRITER,
        }

        for prof in professions:
            qid = mappings.get(prof, None)
            if qid:
                qids.append(qid)

        return qids

    def _populate_person(self, person_entity: imdb_entity.ImdbPersonEntity,
                         person_info: Dict,
                         session: object):
        """
        Given an instance of
        :ref:`soweego.importer.models.imdb_entity.ImdbPersonEntity`
        this function populates its attributes according to
        the provided `person_info` dictionary. It then adds
        said instance to the SQLAlchemy session.

        :param person_entity: the entity which we want to populate
        :param person_info: the data we want to populate the
        entity with
        :param session: the SQLAlchemy session to which we will
        add the entity once it is populated.
        """

        # TODO: we can distinguish gender for actor and actress

        person_entity.catalog_id = person_info.get("nconst")
        person_entity.name = person_info.get("primaryName")
        person_entity.tokens = " ".join(
            text_utils.tokenize(person_entity.name))

        person_entity.born = person_info.get("birthYear")
        person_entity.died = person_info.get("deathYear")

        if person_info.get("primaryProfession"):
            person_entity.occupations = self._translate_professions(
                person_info.get("primaryProfession").split()
            )

        session.add(person_entity)

    def _populate_person_movie_relations(self, person_info: Dict,
                                         session: object):

        know_for_titles = person_info.get(
            "knownForTitles").split(",")

        for title in know_for_titles:
            session.add(imdb_entity.ImdbPersonMovieRelationship(
                from_id=person_info.get("nconst"),
                to_id=title
            ))
