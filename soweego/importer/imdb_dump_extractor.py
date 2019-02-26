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
from typing import Dict, List

from tqdm import tqdm

from soweego.commons import text_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import imdb_entity
from soweego.wikidata import vocabulary as vocab

LOGGER = logging.getLogger(__name__)

DUMP_URL_PERSON_INFO = "https://datasets.imdbws.com/name.basics.tsv.gz"
DUMP_URL_MOVIE_INFO = "https://datasets.imdbws.com/title.basics.tsv.gz"


class ImdbDumpExtractor(BaseDumpExtractor):

    # Counters
    n_actors = 0
    n_directors = 0
    n_movies = 0
    n_musicians = 0
    n_persons = 0
    n_producers = 0
    n_writers = 0
    n_misc = 0
    n_person_movie_links = 0

    _sqlalchemy_commit_every = 700

    def get_dump_download_urls(self) -> List[str]:
        """
        :return: the urls from which to download the data dumps
        the first URL is the one for the **person dump**, the
        second downloads the **movie dump**
        """
        return [DUMP_URL_PERSON_INFO, DUMP_URL_MOVIE_INFO]

    def _normalize_null(self, entity: Dict) -> None:
        """
        IMDB represents a null entry with \\N , this method converts
        all \\N to None so that they're saved as null in the database.
        This is done for all "entries" of a given entity.

        The normalization process is done *in place*, so this method
        has no return value.

        :param entity: represents the entity we want to *normalize*
        """

        for key, value in entity.items():
            if value == "\\N":
                entity[key] = None

    def extract_and_populate(self, dump_file_paths: List[str], resolve: bool) -> None:
        """
        Extracts the data in the dumps (person and movie) and processes them.
        It then proceeds to add the appropriate data to the database. See
        :ref:`soweego.importer.models.imdb_entity` module to see the SQLAlchemy
        definition of the entities we use to save IMDB data.

        :param dump_file_paths: the absolute paths of the already downloaded
        dump files.
        """

        person_file_path = dump_file_paths[0]
        movies_file_path = dump_file_paths[1]

        LOGGER.debug("Path to movie info dump: %s", movies_file_path)
        LOGGER.debug("Path to person info dump: %s", person_file_path)

        start = datetime.datetime.now()

        tables = [
            imdb_entity.ImdbActorEntity,
            imdb_entity.ImdbDirectorEntity,
            imdb_entity.ImdbMovieEntity,
            imdb_entity.ImdbMusicianEntity,
            imdb_entity.ImdbProducerEntity,
            imdb_entity.ImdbWriterEntity,
            imdb_entity.ImdbPersonMovieRelationship,
        ]

        db_manager = DBManager()
        LOGGER.info("Connected to database: %s", db_manager.get_engine().url)

        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info("SQL tables dropped and re-created: %s",
                    [table.__tablename__ for table in tables])

        LOGGER.info("Starting import of movies ...")

        # Here we open the movie dump file, and add everything to the DB
        with gzip.open(movies_file_path, "rt") as mdump:

            # count number of rows for TQDM (so we can display how)
            # much is missing to complete the process. Then go back
            # to the start of the file with `.seek(0)`
            n_rows = sum(1 for line in mdump)
            mdump.seek(0)

            LOGGER.debug("Movies dump has %d entries", n_rows)

            session = db_manager.new_session()

            reader = csv.DictReader(mdump, delimiter="\t")
            for movie_info in tqdm(reader, total=n_rows):
                self._normalize_null(movie_info)

                # create the movie SQLAlchemy entity and populate it
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
                    movie_entity.genres = movie_info.get("genres").replace(",", " ")

                session.add(movie_entity)

                # every `_sqlalchemy_commit_every` loops we commit the session to
                # the DB. This is more efficient than commiting every loop, and
                # is not so hard on the memory requirements as would be
                # adding everything to session and commiting once the for loop
                # is done
                if self.n_movies % self._sqlalchemy_commit_every == 0:
                    session.commit()

                self.n_movies += 1

            # commit remaining entities
            session.commit()

        end = datetime.datetime.now()
        LOGGER.info("Movie import completed in %s. "
                    "Total movies imported: %d",
                    end - start, self.n_movies)


        LOGGER.info("Starting import of people ...")

        # reset timer for persons import
        start = datetime.datetime.now()

        # read person dump and add everything to DB
        with gzip.open(person_file_path, "rt") as pdump:

            # get number of rows for proper TQDM process display, then
            # go back to the start of the file
            n_rows = sum(1 for line in pdump)
            pdump.seek(0)

            LOGGER.debug("People dump has %d entries", n_rows)

            session = db_manager.new_session()

            reader = csv.DictReader(pdump, delimiter="\t")
            for person_info in tqdm(reader, total=n_rows):
                self._normalize_null(person_info)

                # IMDb saves the list of professions as a comma separated
                # string
                professions = person_info.get("primaryProfession")

                # if person has no professions then ignore it
                if not professions:
                    LOGGER.debug("Person %s has no professions", person_info.get("nconst"))
                    continue

                professions = professions.split(",")

                # each person can be added to multiple tables in the DB,
                # each table stands for a profession
                types_of_entities = []

                if "actor" in professions or "actress" in professions:
                    self.n_actors += 1
                    types_of_entities.append(imdb_entity.ImdbActorEntity())

                if "director" in professions:
                    self.n_directors += 1
                    types_of_entities.append(imdb_entity.ImdbDirectorEntity())

                if "producer" in professions:
                    self.n_producers += 1
                    types_of_entities.append(imdb_entity.ImdbProducerEntity())

                if any(prof in ["sound_department", "composer",
                                "music_department", "soundtrack"]
                       for prof in professions):
                    self.n_musicians += 1
                    types_of_entities.append(imdb_entity.ImdbMusicianEntity())

                if "writer" in professions:
                    self.n_writers += 1
                    types_of_entities.append(imdb_entity.ImdbWriterEntity())

                # if the only profession a person has is `miscellaneous` then we
                # add it to all tables
                if professions == "miscellaneous":
                    self.n_misc += 1
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
                # database as well
                if person_info.get("knownForTitles"):
                    self.n_person_movie_links += 1
                    self._populate_person_movie_relations(person_info, session)

                # commit results to the database every `_sqlalchemy_commit_every` loops
                if self.n_persons % self._sqlalchemy_commit_every == 0:
                    session.commit()

                self.n_persons += 1

            # finally commit remaining entities
            session.commit()

        end = datetime.datetime.now()
        LOGGER.info("Person import completed in %s. "
                    "Total people imported: %d - "
                    "Actors: %d - Directors: %d - Musicians: %d - "
                    "Producers: %d - Writers: %d - Misc: %d",
                    end - start, self.n_persons, self.n_actors,
                    self.n_directors, self.n_musicians, self.n_producers,
                    self.n_writers, self.n_misc)

    def _populate_person(self, person_entity: imdb_entity.ImdbPersonEntity,
                         person_info: Dict,
                         session: object) -> None:
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

        person_entity.catalog_id = person_info.get("nconst")
        person_entity.name = person_info.get("primaryName")
        person_entity.name_tokens = " ".join(
            text_utils.tokenize(person_entity.name))

        # If either `actor` or `actress` in primary profession
        # (which is a comma separated string of professions)
        # then we can distinguish the gender
        if any(prof in person_info.get("primaryProfession")
               for prof in ["actor", "actress"]):
            person_entity.gender = "male" if "actor" in person_info.get(
                "primaryProfession") else "female"

        # datetime.date(year, month, day)
        born_year = person_info.get("birthYear")
        if born_year:
            person_entity.born = datetime.date(int(born_year), 1, 1)

        death_year = person_info.get("deathYear")
        if death_year:
            person_entity.died = datetime.date(int(death_year), 1, 1)

        # The array of primary professions gets translated to a list
        # of the QIDs that represent said professions in Wikidata
        if person_info.get("primaryProfession"):
            person_entity.occupations = " ".join(self._translate_professions(
                person_info.get("primaryProfession").split(",")
            ))

        session.add(person_entity)

    def _populate_person_movie_relations(self, person_info: Dict,
                                         session: object) -> None:
        """
        Given a `person_info` we extract the ID that the person has
        in IMDB and the IDs of the movies for which this person is
        known (which also come from IMDB). We add a
        :ref:`soweego.importer.models.imdb_entity.ImdbPersonMovieRelationship`
        entity to the session for each realtion.

        :param person_info: dictionary that contains the IMDB person ID and
        the IMDB movie IDs (for movies the specific person is known). The movie
        IDs are a comma separated string
        :param session: the SQLAlchemy session to which we will
        add the relation entities.
        """

        know_for_titles = person_info.get(
            "knownForTitles").split(",")

        for title in know_for_titles:

            session.add(imdb_entity.ImdbPersonMovieRelationship(
                from_catalog_id=person_info.get("nconst"),
                to_catalog_id=title
            ))

    def _translate_professions(self, professions: List[str]) -> List[str]:
        """
        Gets the list of professions (as a list of strings) directly from IMDb
        and translates these to a list of Wikidata QIDs for each specific
        profession. Unmappable professions (like `miscellaneous` are removed)

        The actual QIDs and the dictionary where this mapping is
        encoded can both be found in
        :ref:`soweego.wikidata.vocabulary`

        :param professions: list of profession names, given by IMDB

        :return: list of QIDs for said professions
        """
        qids = []

        for prof in professions:
            qid = vocab.IMDB_PROFESSIONS_MAPPINGS.get(prof, None)
            if qid:
                qids.append(qid)

        return qids
