#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`IMDb <https://www.imdb.com/>`_ dump extractor."""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, tupini07'

import copy
import csv
import datetime
import gzip
import logging
from typing import Dict, Generator, List, Tuple

from soweego.commons import text_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import imdb_entity
from soweego.wikidata import vocabulary as vocab
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)

DUMP_URL_PERSON_INFO = 'https://datasets.imdbws.com/name.basics.tsv.gz'
DUMP_URL_MOVIE_INFO = 'https://datasets.imdbws.com/title.basics.tsv.gz'


class IMDbDumpExtractor(BaseDumpExtractor):
    """Download IMDb dumps, extract data, and
    populate a database instance.
    """

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

    _sqlalchemy_commit_every = 100_000

    def get_dump_download_urls(self) -> List[str]:
        return [DUMP_URL_PERSON_INFO, DUMP_URL_MOVIE_INFO]

    @staticmethod
    def _normalize_null(entity: Dict) -> None:
        """IMDb represents a null entry with \\N , this method converts
        all \\N to None so that they're saved as null in the database.
        This is done for all 'entries' of a given entity.

        The normalization process is done *in place*, so this method
        has no return value.

        :param entity: represents the entity we want to *normalize*
        """

        for key, value in entity.items():
            if value == '\\N':
                entity[key] = None

    def extract_and_populate(
        self, dump_file_paths: List[str], resolve: bool
    ) -> None:
        """Extract relevant data from the *name* (people) and *title* (works)
        IMDb dumps, preprocess them, populate
        `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities, and persist
        them to a database instance.

        See :mod:`~soweego.importer.models.imdb_entity`
        for the ORM definitions.

        :param dump_file_paths: paths to downloaded catalog dumps
        :param resolve: whether to resolve URLs found in catalog dumps or not
        """

        # the order of these files is specified in `self.get_dump_download_urls`
        person_file_path = dump_file_paths[0]
        movies_file_path = dump_file_paths[1]

        LOGGER.debug('Path to movie info dump: %s', movies_file_path)
        LOGGER.debug('Path to person info dump: %s', person_file_path)

        start = datetime.datetime.now()

        tables = [
            imdb_entity.IMDbActorEntity,
            imdb_entity.IMDbDirectorEntity,
            imdb_entity.IMDbTitleEntity,
            imdb_entity.IMDbMusicianEntity,
            imdb_entity.IMDbProducerEntity,
            imdb_entity.IMDbWriterEntity,
            imdb_entity.IMDbTitleNameRelationship,
        ]

        db_manager = DBManager()
        LOGGER.info('Connected to database: %s', db_manager.get_engine().url)

        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info(
            'SQL tables dropped and re-created: %s',
            [table.__tablename__ for table in tables],
        )

        LOGGER.info('Starting import of movies ...')

        # Here we open the movie dump file, and add everything to the DB
        for movie_info, entity_array in self._loop_through_entities(
            movies_file_path
        ):

            # create the movie SQLAlchemy entity and populate it
            movie_entity = imdb_entity.IMDbTitleEntity()
            movie_entity.catalog_id = movie_info.get('tconst')
            movie_entity.title_type = movie_info.get('titleType')
            if movie_info.get('primaryTitle') is not None:
                movie_entity.name = movie_info.get('primaryTitle')
                movie_entity.name_tokens = ' '.join(
                    text_utils.tokenize(movie_info.get('primaryTitle'))
                )
            movie_entity.is_adult = (
                True if movie_info.get('isAdult') == '1' else False
            )
            try:
                movie_entity.born = datetime.date(
                    year=int(movie_info.get('startYear')), month=1, day=1
                )
                movie_entity.born_precision = 9
            except (KeyError, TypeError):
                LOGGER.debug('No start year value for %s', movie_entity)
            try:
                movie_entity.died = datetime.date(
                    year=int(movie_info.get('endYear')), month=1, day=1
                )
                movie_entity.died_precision = 9
            except (KeyError, TypeError):
                LOGGER.debug('No end year value for %s', movie_entity)
            movie_entity.runtime_minutes = movie_info.get('runtimeMinutes')

            if movie_info.get('genres'):  # if movie has a genre specified
                movie_entity.genres = ' '.join(
                    text_utils.tokenize(movie_info.get('genres'))
                )

            # Creates entity for alias
            alias = movie_info.get('originalTitle')
            if alias is not None and movie_entity.name != alias:
                alias_entity = copy.deepcopy(movie_entity)
                alias_entity.name = alias
                alias_entity.name_tokens = ' '.join(text_utils.tokenize(alias))
                entity_array.append(alias_entity)

            entity_array.append(movie_entity)

            self.n_movies += 1

        # mark end for movie import process
        end = datetime.datetime.now()
        LOGGER.info(
            'Movie import completed in %s. ' 'Total movies imported: %d',
            end - start,
            self.n_movies,
        )

        LOGGER.info('Starting import of people ...')

        # reset timer for persons import
        start = datetime.datetime.now()

        for person_info, entity_array in self._loop_through_entities(
            person_file_path
        ):

            # IMDb saves the list of professions as a comma separated
            # string
            professions = person_info.get('primaryProfession')

            # if person has no professions then ignore it
            if not professions:
                LOGGER.debug(
                    'Person %s has no professions', person_info.get('nconst')
                )
                continue

            professions = professions.split(',')

            # each person can be added to multiple tables in the DB,
            # each table stands for one of the main professions
            types_of_entities = []

            if 'actor' in professions or 'actress' in professions:
                self.n_actors += 1
                types_of_entities.append(imdb_entity.IMDbActorEntity())

            if 'director' in professions:
                self.n_directors += 1
                types_of_entities.append(imdb_entity.IMDbDirectorEntity())

            if 'producer' in professions:
                self.n_producers += 1
                types_of_entities.append(imdb_entity.IMDbProducerEntity())

            if any(
                prof
                in [
                    'sound_department',
                    'composer',
                    'music_department',
                    'soundtrack',
                ]
                for prof in professions
            ):
                self.n_musicians += 1
                types_of_entities.append(imdb_entity.IMDbMusicianEntity())

            if 'writer' in professions:
                self.n_writers += 1
                types_of_entities.append(imdb_entity.IMDbWriterEntity())

            # if the only profession a person has is `miscellaneous` then we
            # add it to all tables
            if professions == ['miscellaneous']:
                self.n_misc += 1
                types_of_entities = [
                    imdb_entity.IMDbActorEntity(),
                    imdb_entity.IMDbDirectorEntity(),
                    imdb_entity.IMDbMusicianEntity(),
                    imdb_entity.IMDbProducerEntity(),
                    imdb_entity.IMDbWriterEntity(),
                ]

            # add person to every matching table
            for etype in types_of_entities:
                self._populate_person(etype, person_info, entity_array)

            # if person is known for any movies then add these to the
            # database as well
            if person_info.get('knownForTitles'):
                self.n_person_movie_links += 1
                self._populate_person_movie_relations(person_info, entity_array)

            self.n_persons += 1

        # mark the end time for the person import process
        end = datetime.datetime.now()
        LOGGER.info(
            'Person import completed in %s. '
            'Total people imported: %d - '
            'Actors: %d - Directors: %d - Musicians: %d - '
            'Producers: %d - Writers: %d - Misc: %d',
            end - start,
            self.n_persons,
            self.n_actors,
            self.n_directors,
            self.n_musicians,
            self.n_producers,
            self.n_writers,
            self.n_misc,
        )

    def _loop_through_entities(
        self, file_path: str
    ) -> Generator[Tuple[Dict, List], None, None]:
        """
        Generator that given an IMDb dump file (which
        should be ".tsv.gz" format) it loops through every
        entry and yields it.

        :return: a generator which yields a Tuple[entity_info, entity_array]
        the consumer of this generator will take `entity_info`, create an
        SQLAlchemy entity, and append this to the `entity_array`
        """
        db_manager = DBManager()

        with gzip.open(file_path, 'rt') as ddump:
            session = db_manager.new_session()

            # count number of rows for TQDM, so we can display how
            # much is missing to complete the process. Then go back
            # to the start of the file with `.seek(0)`
            n_rows = sum(1 for line in ddump)
            ddump.seek(0)

            entity_array = []
            LOGGER.debug('Dump "%s" has %d entries', file_path, n_rows)

            reader = csv.DictReader(ddump, delimiter='\t')

            # for every entry in the file..
            for entity_info in tqdm(reader, total=n_rows):
                # clean the entry
                self._normalize_null(entity_info)

                # yield the cleaned dict
                yield entity_info, entity_array

                # every `_sqlalchemy_commit_every` loops we commit the
                # session to the DB. This is more efficient than commiting
                # every loop, and is not so hard on the memory requirements
                # as would be adding everything to session and commiting once
                # the for loop is done
                if len(entity_array) >= self._sqlalchemy_commit_every:
                    LOGGER.info(
                        'Adding batch of entities to the database, '
                        'this will take a while. Progress will resume soon.'
                    )

                    insert_start_time = datetime.datetime.now()

                    session.bulk_save_objects(entity_array)
                    session.commit()
                    session.expunge_all()  # clear session

                    entity_array.clear()  # clear entity array

                    LOGGER.debug(
                        'It took %s to add %s entities to the database',
                        datetime.datetime.now() - insert_start_time,
                        len(entity_array),
                    )

            # commit remaining entities
            session.bulk_save_objects(entity_array)
            session.commit()

            # clear list reference since it might still be available in
            # the scope where this generator was used.
            entity_array.clear()

    def _populate_person(
        self,
        person_entity: imdb_entity.IMDbNameEntity,
        person_info: Dict,
        entity_array: object,
    ) -> None:
        """
        Given an instance of
        :ref:`soweego.importer.models.imdb_entity.IMDbNameEntity`
        this function populates its attributes according to
        the provided `person_info` dictionary. It then adds
        said instance to the SQLAlchemy session.

        :param person_entity: the entity which we want to populate
        :param person_info: the data we want to populate the
        entity with
        :param entity_array: an external array to which we'll add the
        entity once it is populated.
        """

        person_entity.catalog_id = person_info.get('nconst')
        person_entity.name = person_info.get('primaryName')
        person_entity.name_tokens = ' '.join(
            text_utils.tokenize(person_entity.name)
        )

        # If either `actor` or `actress` in primary profession
        # (which is a comma separated string of professions)
        # then we can distinguish the gender
        if any(
            prof in person_info.get('primaryProfession')
            for prof in ['actor', 'actress']
        ):
            person_entity.gender = (
                'male'
                if 'actor' in person_info.get('primaryProfession')
                else 'female'
            )

        # IMDb only provides us with the birth and death year of
        # a person, so this is the only one we'll take into
        # account. Month and Day are set by default to 1. The
        # base `IMDbNameEntity` defines a precision of 9 for the
        # birth and death dates, which (according to
        # `vocab.DATE_PRECISION`) means that only the year is correct.
        born_year = person_info.get('birthYear')
        if born_year:
            # datetime.date(year, month, day)
            person_entity.born = datetime.date(int(born_year), 1, 1)

        death_year = person_info.get('deathYear')
        if death_year:
            person_entity.died = datetime.date(int(death_year), 1, 1)

        # The array of primary professions gets translated to a list
        # of the QIDs that represent said professions in Wikidata
        if person_info.get('primaryProfession'):
            # get QIDs of occupations for person
            translated_occupations = self._translate_professions(
                person_info.get('primaryProfession').split(',')
            )

            # only save those occupations which are not the main
            # occupation of the entity type (ie, for ActorEntity
            # don't include 'actor' occupation since it is implicit)
            person_entity.occupations = ' '.join(
                occ
                for occ in translated_occupations
                if occ != person_entity.table_occupation
            )

        entity_array.append(person_entity)

    @staticmethod
    def _populate_person_movie_relations(
        person_info: Dict, entity_array: object
    ) -> None:
        """
        Given a `person_info` we extract the ID that the person has
        in IMDB and the IDs of the movies for which this person is
        known (which also come from IMDB). We add a
        :ref:`soweego.importer.models.imdb_entity.ImdbPersonMovieRelationship`
        entity to the session for each relation.

        :param person_info: dictionary that contains the IMDB person ID and
        the IMDB movie IDs (for movies the specific person is known). The movie
        IDs are a comma separated string
        :param entity_array: an external array to which we'll add the
        person-movie relations.
        """

        know_for_titles = person_info.get('knownForTitles').split(',')

        for title in know_for_titles:
            entity_array.append(
                imdb_entity.IMDbTitleNameRelationship(
                    from_catalog_id=title,
                    to_catalog_id=person_info.get('nconst'),
                )
            )

    @staticmethod
    def _translate_professions(professions: List[str]) -> List[str]:
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
            qid = vocab.IMDB_PROFESSIONS_MAPPING.get(prof, None)
            if qid:
                qids.append(qid)

        return qids
