#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""IMDB dump extractor"""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, tupini07'

import os
import csv
import gzip
import logging
import datetime
from typing import Dict

import requests

from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import imdb_entity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.commons import http_client as client


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

    def get_dump_download_url(self) -> str:
        return DUMP_URL_PERSON_INFO

    def _download_movies_dataset(self, dump_file_path: str):

        filename = DUMP_URL_MOVIE_INFO.split("/")[-1]

        # the files get updated once each day, so we downloa the file for today
        filename = datetime.date.today().strftime('%d-%m-%Y-') + filename

        path_to_download = "/".join(dump_file_path.split("/")
                                    [:-1]) + "/" + filename

        # Check if the current dump is up-to-date
        if not os.path.isfile(path_to_download):
            client.download_file(DUMP_URL_MOVIE_INFO, path_to_download)

        return path_to_download

    def _normalize_null(self, entity: Dict):
        """
        IMDB represents a null entry with \\N , we want to convert
        all \\N to None so that they're saved as null in the database
        """

        for k, v in entity.items():
            if v == "\\N":
                entity[k] = None

    def extract_and_populate(self, dump_file_path: str):

        start = datetime.datetime.now()

        tables = [
            # imdb_entity.ImdbMovieEntity,
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

        LOGGER.info("Downloading movie dataset")

        movies_file_path = self._download_movies_dataset(dump_file_path)

        LOGGER.info("Movie dataset has been downloaded to '%s'",
                    movies_file_path)

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

        LOGGER.info("Starting import persons from IMDB dump '%s'",
                    dump_file_path)

        with gzip.open(dump_file_path, "rt") as pdump:
            reader = csv.DictReader(pdump, delimiter="\t")
            for person_info in reader:
                self._normalize_null(person_info)

                professions = person_info.get("primaryProfession")

                # if person has no professions then ignore it
                if not professions:
                    continue

                professions = professions.split(",")

                session = db_manager.new_session()

                types_of_entities = []
                if "actor" in professions or "actress" in professions:
                    types_of_entities.append(imdb_entity.ImdbActorEntity())

                if "director" in professions:
                    types_of_entities.append(imdb_entity.ImdbDirectorEntity())

                if "producer" in professions:
                    types_of_entities.append(imdb_entity.ImdbProducerEntity())

                if "writer" in professions:
                    types_of_entities.append(imdb_entity.ImdbWriterEntity())

                for etype in types_of_entities:
                    self._populate_person(etype, person_info, session)

                if person_info.get("knownForTitles"):
                    self._populate_person_movie_relations(person_info, session)

                session.commit()

    def _populate_person(self, person_entity: imdb_entity.ImdbPersonEntity, person_info: Dict, session: object):
        person_entity.catalog_id = person_info.get("nconst")
        person_entity.name = person_info.get("primaryName")
        person_entity.tokens = " ".join(
            text_utils.tokenize(person_entity.name))

        person_entity.born = person_info.get("birthYear")
        person_entity.died = person_info.get("deathYear")

        if person_info.get("primaryProfession"):
            person_entity.occupations = person_info.get(
                "primaryProfession").split()

        session.add(person_entity)

    def _populate_person_movie_relations(self, person_info: Dict, session: object):
        know_for_titles = person_info.get(
            "knownForTitles").split(",")

        for title in know_for_titles:
            session.add(imdb_entity.ImdbPersonMovieRelationship(
                from_id=person_info.get("nconst"),
                to_id=title
            ))
