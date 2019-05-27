#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz dump extractor"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

import logging
import os
import shutil
import tarfile
from collections import defaultdict
from csv import DictReader
from datetime import date, datetime
from typing import Iterable, Tuple

import requests
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.commons.utils import count_num_lines_in_file
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.musicbrainz_entity import (
    MusicBrainzArtistBandRelationship,
    MusicbrainzArtistEntity,
    MusicbrainzArtistLinkEntity,
    MusicbrainzBandEntity,
    MusicbrainzBandLinkEntity,
    MusicBrainzReleaseGroupArtistRelationship,
    MusicbrainzReleaseGroupEntity,
    MusicbrainzReleaseGroupLinkEntity,
)
from soweego.wikidata.sparql_queries import external_id_pids_and_urls_query

LOGGER = logging.getLogger(__name__)


class MusicBrainzDumpExtractor(BaseDumpExtractor):
    """Defines where to download Musicbrainz dump and how to post-process it"""

    _sqlalchemy_commit_every = 1_000_000

    def get_dump_download_urls(self) -> Iterable[str]:
        base_url = 'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport'
        latest_version = requests.get(f'{base_url}/LATEST').text.rstrip()
        return [f'{base_url}/%s/mbdump.tar.bz2' % latest_version]

    def extract_and_populate(
        self, dump_file_paths: Iterable[str], resolve: bool
    ):
        dump_file_path = dump_file_paths[0]
        dump_path = os.path.join(
            os.path.dirname(os.path.abspath(dump_file_path)),
            f"{os.path.basename(dump_file_path)}_extracted",
        )

        if not os.path.isdir(dump_path):
            with tarfile.open(dump_file_path, "r:bz2") as tar:
                LOGGER.info(
                    "Extracting dump %s in %s", dump_file_path, dump_path
                )
                tar.extractall(dump_path)
                LOGGER.info(
                    "Extracted dump %s in %s", dump_file_path, dump_path
                )

        db_manager = DBManager()

        tables = [MusicbrainzReleaseGroupEntity]
        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing release groups")

        release_groups_count = self._add_entities_from_generator(
            db_manager, self._release_group_generator, dump_path
        )

        LOGGER.debug("Added %s/%s release group records", *release_groups_count)

        def release_artist_relationships_uniqueness_filter():
            yield from [
                MusicBrainzReleaseGroupArtistRelationship(item[0], item[1])
                for item in set(
                    self._release_group_artist_relationship_generator(dump_path)
                )
            ]

        tables = [MusicBrainzReleaseGroupArtistRelationship]
        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing relationships release-artist/band")

        relationships_count = self._add_entities_from_generator(
            db_manager, release_artist_relationships_uniqueness_filter
        )

        LOGGER.debug("Added %s/%s relationships records", *relationships_count)

        tables = [MusicbrainzReleaseGroupLinkEntity]
        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing release groups links")

        link_count = self._add_entities_from_generator(
            db_manager, self._release_group_link_generator, dump_path, resolve
        )

        LOGGER.debug("Added %s/%s release group link records", *link_count)

        tables = [MusicbrainzArtistEntity, MusicbrainzBandEntity]
        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing artists and bands")

        artist_count = self._add_entities_from_generator(
            db_manager, self._artist_generator, dump_path
        )

        LOGGER.debug("Added %s/%s artist records", *artist_count)

        tables = [MusicbrainzArtistLinkEntity, MusicbrainzBandLinkEntity]
        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing links")

        link_count = self._add_entities_from_generator(
            db_manager, self._artist_link_generator, dump_path, resolve
        )

        LOGGER.debug("Added %s/%s link records", *link_count)
        LOGGER.info("Importing ISNIs")

        isni_link_count = self._add_entities_from_generator(
            db_manager, self._isni_link_generator, dump_path, resolve
        )

        LOGGER.debug("Added %s/%s ISNI link records", *isni_link_count)

        tables = [MusicBrainzArtistBandRelationship]
        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info(
            "Dropped and created tables %s",
            [table.__tablename__ for table in tables],
        )
        LOGGER.info("Importing relationships artist-band")

        def artist_band_relationships_uniqueness_filter():
            yield from [
                MusicBrainzArtistBandRelationship(item[0], item[1])
                for item in set(
                    self._artist_band_relationship_generator(dump_path)
                )
            ]

        relationships_count = self._add_entities_from_generator(
            db_manager, artist_band_relationships_uniqueness_filter
        )

        LOGGER.debug("Added %s/%s relationships records", *relationships_count)
        shutil.rmtree(dump_path, ignore_errors=True)

    def _add_entities_from_generator(
        self, db_manager, generator_, *args
    ) -> Tuple[int, int]:
        """
        Adds all entities yielded by a generator to the DB

        :return: (the total number of entities yielded,
        the number of entities added to the DB)
        """

        # we keep track of both the total number of entities added
        # to the database and the total number of entities the
        # generator yields. If everything goes ok then these 2
        # numbers should be the same
        n_total_entities = 0
        n_added_entities = 0

        session = db_manager.new_session()

        entity_array = []  # array to which we'll add the entities

        # the generator will give us a new entity each loop
        # so we just add this to the `entity_array` and commit
        # it once it is large enough (self._sqlalchemy_commit_every)
        for entity in generator_(*args):

            try:
                n_total_entities += 1
                entity_array.append(entity)

                # commit entities to DB in batches, it is mode
                # efficient
                if len(entity_array) >= self._sqlalchemy_commit_every:
                    LOGGER.info(
                        "Adding batch of entities to the database, "
                        "this will take a while. Progress will "
                        "resume soon."
                    )

                    insert_start_time = datetime.now()

                    session.bulk_save_objects(entity_array)
                    session.commit()
                    session.expunge_all()  # clear session

                    entity_array.clear()  # clear entity array

                    LOGGER.debug(
                        "It took %s to add %s entities to the database",
                        datetime.now() - insert_start_time,
                        len(entity_array),
                    )

                n_added_entities += 1

            except IntegrityError as i:
                LOGGER.warning(str(i))

        # finally, commit remaining entities in session
        session.bulk_save_objects(entity_array)
        session.commit()

        # and close session
        session.close()

        return n_total_entities, n_added_entities

    @staticmethod
    def _get_urls_for_entity_id(
        dump_path: str, l_path: str, resolve: bool
    ) -> dict:
        """given a l_{something}_url relationship file, return a dict of
        somethingid-[urls]"""

        LOGGER.info(f"Loading %s relationships", l_path)

        urlid_entityid_relationship = {}

        with open(l_path, "r") as tsvfile:
            url_relationships = DictReader(
                tsvfile, delimiter='\t', fieldnames=[i for i in range(0, 6)]
            )

            for relationship in tqdm(
                url_relationships, total=count_num_lines_in_file(tsvfile)
            ):
                # url id matched with its user id
                if relationship[3] in urlid_entityid_relationship:
                    LOGGER.warning(
                        'Url with ID %s has multiple entities, only one will '
                        'be stored',
                        relationship[3],
                    )
                else:
                    urlid_entityid_relationship[relationship[3]] = relationship[
                        2
                    ]

        url_path = os.path.join(dump_path, 'mbdump', 'url')
        url_entityid = {}

        LOGGER.info('Checking URLs related to entity')

        # Translates URL IDs to the relative URL
        with open(url_path, "r") as tsvfile:

            urls = DictReader(
                tsvfile, delimiter='\t', fieldnames=[i for i in range(0, 5)]
            )

            for url_record in tqdm(
                urls, total=count_num_lines_in_file(tsvfile)
            ):

                urlid = url_record[0]
                if urlid in urlid_entityid_relationship:
                    for candidate_url in url_utils.clean(url_record[2]):
                        if not url_utils.validate(candidate_url):
                            continue
                        if resolve and not url_utils.resolve(candidate_url):
                            continue
                        url_entityid[
                            candidate_url
                        ] = urlid_entityid_relationship[urlid]
                        del urlid_entityid_relationship[urlid]

        entityid_url = defaultdict(list)
        # Inverts dictionary
        for url, entityid in url_entityid.items():
            entityid_url[entityid].append(url)

        return entityid_url

    def _artist_link_generator(self, dump_path: str, resolve: bool):
        l_artist_url_path = os.path.join(dump_path, 'mbdump', 'l_artist_url')

        # Loads all the relationships between URL and ARTIST ID
        artistid_url = self._get_urls_for_entity_id(
            dump_path, l_artist_url_path, resolve
        )

        LOGGER.info('Adding link entities to DB')
        # Translates ARTIST ID to the relative ARTIST
        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:

            n_rows = count_num_lines_in_file(artistfile)
            artist_link_reader = DictReader(
                artistfile,
                delimiter='\t',
                fieldnames=[
                    'id',
                    'gid',
                    'label',
                    'sort_label',
                    'b_year',
                    'b_month',
                    'b_day',
                    'd_year',
                    'd_month',
                    'd_day',
                    'type_id',
                ],
            )

            for artist in tqdm(artist_link_reader, total=n_rows):

                if artist['id'] in artistid_url:
                    for link in artistid_url[artist['id']]:
                        if self._check_person(artist['type_id']):
                            current_entity = MusicbrainzArtistLinkEntity()
                            self._fill_link_entity(
                                current_entity, artist['gid'], link
                            )
                            yield current_entity
                        if self._check_band(artist['type_id']):
                            current_entity = MusicbrainzBandLinkEntity()
                            self._fill_link_entity(
                                current_entity, artist['gid'], link
                            )
                            yield current_entity

    def _release_group_link_generator(self, dump_path: str, resolve: bool):
        l_release_group_url_path = os.path.join(
            dump_path, 'mbdump', 'l_release_group_url'
        )

        release_group_id_urls = self._get_urls_for_entity_id(
            dump_path, l_release_group_url_path, resolve
        )

        release_group_path = os.path.join(dump_path, 'mbdump', 'release_group')
        with open(release_group_path) as rfile:
            n_rows = count_num_lines_in_file(rfile)
            releases = DictReader(
                rfile, delimiter='\t', fieldnames=['id', 'gid', 'label']
            )

            for release in tqdm(releases, total=n_rows):
                if release['id'] in release_group_id_urls:
                    for link in release_group_id_urls[release['id']]:
                        entity = MusicbrainzReleaseGroupLinkEntity()
                        self._fill_link_entity(entity, release['gid'], link)
                        yield entity

    def _isni_link_generator(self, dump_path: str, resolve: bool):
        isni_file_path = os.path.join(dump_path, 'mbdump', 'artist_isni')

        artist_link = {}

        done = False
        for result in external_id_pids_and_urls_query():
            if done:
                break
            for pid, formatter in result.items():
                if pid != 'P213':
                    continue
                for url_formatter, _ in formatter.items():
                    with open(isni_file_path, 'r') as artistfile:
                        for artistid_isni in DictReader(
                            artistfile,
                            delimiter='\t',
                            fieldnames=['id', 'isni'],
                        ):
                            # If ISNI is valid, generates an url
                            artistid = artistid_isni['id']
                            isni = artistid_isni['isni']

                            link = url_formatter.replace('$1', isni)
                            for candidate_url in url_utils.clean(link):
                                if not url_utils.validate(candidate_url):
                                    continue
                                if resolve and not url_utils.resolve(
                                    candidate_url
                                ):
                                    continue
                                artist_link[artistid] = candidate_url
                done = True

        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:

            n_rows = count_num_lines_in_file(artistfile)

            artist_isni_reader = DictReader(
                artistfile,
                delimiter='\t',
                fieldnames=[
                    'id',
                    'gid',
                    'label',
                    'sort_label',
                    'b_year',
                    'b_month',
                    'b_day',
                    'd_year',
                    'd_month',
                    'd_day',
                    'type_id',
                ],
            )

            for artist in tqdm(artist_isni_reader, total=n_rows):
                try:
                    # Checks if artist has isni
                    link = artist_link[artist['id']]
                    if self._check_person(artist['type_id']):
                        current_entity = MusicbrainzArtistLinkEntity()
                        self._fill_link_entity(
                            current_entity, artist['gid'], link
                        )
                        yield current_entity
                    if self._check_band(artist['type_id']):
                        current_entity = MusicbrainzBandLinkEntity()
                        self._fill_link_entity(
                            current_entity, artist['gid'], link
                        )
                        yield current_entity
                except KeyError:
                    continue

    def _artist_generator(self, dump_path):
        artist_alias_path = os.path.join(dump_path, 'mbdump', 'artist_alias')
        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        area_path = os.path.join(dump_path, 'mbdump', 'area')

        aliases = defaultdict(list)
        areas = {}

        LOGGER.info('Getting artist aliases')

        # Key is the entity id which has a list of aliases
        with open(artist_alias_path, 'r') as aliasesfile:
            for alias in DictReader(
                aliasesfile,
                delimiter='\t',
                fieldnames=['id', 'parent_id', 'label'],
            ):
                aliases[alias['parent_id']].append(alias['label'])

        LOGGER.info('Getting area IDs and related names')

        # Key is the area internal id, value is the name
        with open(area_path, 'r') as areafile:
            for area in DictReader(
                areafile, delimiter='\t', fieldnames=['id', 'gid', 'name']
            ):
                areas[area['id']] = area['name'].lower()

        LOGGER.info('Importing artist entities into DB')

        with open(artist_path, 'r') as artistfile:

            n_rows = count_num_lines_in_file(artistfile)

            artist_reader = DictReader(
                artistfile,
                delimiter='\t',
                fieldnames=[
                    'id',
                    'gid',
                    'label',
                    'sort_label',
                    'b_year',
                    'b_month',
                    'b_day',
                    'd_year',
                    'd_month',
                    'd_day',
                    'type_id',
                    'area',
                    'gender',
                    'ND1',
                    'ND2',
                    'ND3',
                    'ND4',
                    'b_place',
                    'd_place',
                ],
            )

            for artist in tqdm(artist_reader, total=n_rows):
                if self._check_person(artist['type_id']):
                    current_entity = MusicbrainzArtistEntity()

                    try:
                        self._fill_entity(current_entity, artist, areas)
                        current_entity.gender = self._artist_gender(
                            artist['gender']
                        )
                    except KeyError:
                        LOGGER.error('Wrong gender code: %s', artist)
                        continue

                    # Creates an entity foreach available alias
                    for alias in self._alias_entities(
                        current_entity,
                        MusicbrainzArtistEntity,
                        aliases[artist['id']],
                    ):
                        alias.gender = current_entity.gender
                        yield alias

                    yield current_entity

                if self._check_band(artist['type_id']):
                    current_entity = MusicbrainzBandEntity()

                    try:
                        self._fill_entity(current_entity, artist, areas)
                    except ValueError:
                        LOGGER.error('Wrong date: %s', artist)
                        continue

                    # Creates an entity foreach available alias
                    for alias in self._alias_entities(
                        current_entity,
                        MusicbrainzBandEntity,
                        aliases[artist['id']],
                    ):
                        yield alias

                    yield current_entity

    @staticmethod
    def _artist_band_relationship_generator(dump_path):
        link_types = set(['855', '103', '305', '965', '895'])
        link_file_path = os.path.join(dump_path, 'mbdump', 'link')
        to_invert = set()

        LOGGER.info('Loading artist-band relationships')

        links = set()
        with open(link_file_path) as link_file:
            reader = DictReader(
                link_file, delimiter='\t', fieldnames=['id', 'link_type']
            )
            for row in reader:
                if row['link_type'] in link_types:
                    links.add(row['id'])

        artists_relationship_file = os.path.join(
            dump_path, 'mbdump', 'l_artist_artist'
        )

        ids_translator = {}
        relationships = []
        with open(artists_relationship_file) as relfile:
            reader = DictReader(
                relfile,
                delimiter='\t',
                fieldnames=['id', 'link_id', 'entity0', 'entity1'],
            )
            for row in reader:
                link_id = row['link_id']
                if link_id in links:
                    en0 = row['entity0']
                    en1 = row['entity1']
                    ids_translator[en0] = ''
                    ids_translator[en1] = ''
                    relationship = (en0, en1)
                    relationships.append(relationship)
                    if link_id == '855':
                        to_invert.add(relationship)

        # To hope in Garbage collection intervention
        links = None

        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(
                artistfile, delimiter='\t', fieldnames=['id', 'gid']
            ):
                if artist['id'] in ids_translator:
                    ids_translator[artist['id']] = artist['gid']

        LOGGER.info('Adding relationships into DB')

        for relation in tqdm(relationships):
            translation0, translation1 = (
                ids_translator[relation[0]],
                ids_translator[relation[1]],
            )

            if translation0 and translation1:
                if relation in to_invert:
                    yield (translation1, translation0)
                else:
                    yield (translation0, translation1)
            else:
                LOGGER.warning(
                    "Artist id missing translation: %s to (%s, %s)",
                    relation,
                    translation0,
                    translation1,
                )

    def _release_group_generator(self, dump_path):
        release_group_datesprec = self._retrieve_release_group_dates(dump_path)
        release_group_path = os.path.join(dump_path, 'mbdump', 'release_group')

        with open(release_group_path, 'r') as releasefile:
            release_reader = DictReader(
                releasefile,
                delimiter='\t',
                fieldnames=['id', 'gid', 'label', 'artist_credit', 'type_id'],
            )

            for row in tqdm(
                release_reader, total=count_num_lines_in_file(releasefile)
            ):
                entity = MusicbrainzReleaseGroupEntity()
                self._fill_entity(entity, row, None)
                if row['id'] in release_group_datesprec:
                    dateprec = release_group_datesprec[row['id']]
                    if dateprec[1] != 0:
                        entity.born_precision = dateprec[1]
                        entity.born = dateprec[0]
                yield entity

    @staticmethod
    def _release_group_artist_relationship_generator(dump_path):
        release_group_path = os.path.join(dump_path, 'mbdump', 'release_group')

        artist_credit_release = defaultdict(list)

        with open(release_group_path, 'r') as releasefile:
            n_rows = count_num_lines_in_file(releasefile)
            release_reader = DictReader(
                releasefile,
                delimiter='\t',
                fieldnames=['id', 'gid', 'label', 'artist_credit', 'type_id'],
            )
            for row in tqdm(release_reader, total=n_rows):
                artist_credit_release[row['artist_credit']].append(row['gid'])

        artist_credit_name_path = os.path.join(
            dump_path, 'mbdump', 'artist_credit_name'
        )

        artist_id_release = defaultdict(list)
        with open(artist_credit_name_path) as artistcreditfile:
            artist_credit_reader = DictReader(
                artistcreditfile,
                delimiter='\t',
                fieldnames=['id', 'nd', 'artist_id', 'artist_name'],
            )

            n_rows = count_num_lines_in_file(artistcreditfile)
            for row in tqdm(artist_credit_reader, total=n_rows):
                artist_id_release[row['artist_id']] = artist_credit_release[
                    row['id']
                ]
                # memory free up for performance
                del artist_credit_release[row['id']]

        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:

            n_rows = count_num_lines_in_file(artistfile)
            artist_link_reader = DictReader(
                artistfile,
                delimiter='\t',
                fieldnames=[
                    'id',
                    'gid',
                    'label',
                    'sort_label',
                    'b_year',
                    'b_month',
                    'b_day',
                    'd_year',
                    'd_month',
                    'd_day',
                    'type_id',
                ],
            )

            for artist in tqdm(artist_link_reader, total=n_rows):
                for release_id in artist_id_release[artist['id']]:
                    yield (release_id, artist['gid'])
                # memory freeup for performance
                del artist_id_release[artist['id']]

    def _fill_entity(self, entity, info, areas):
        entity.catalog_id = info['gid']
        entity.name = info['label']
        name_tokens = text_utils.tokenize(info['label'])
        if name_tokens:
            entity.name_tokens = ' '.join(name_tokens)
        try:
            birth_date = self._get_date_and_precision(
                info['b_year'], info['b_month'], info['b_day']
            )
            entity.born = birth_date[0]
            entity.born_precision = birth_date[1]
        except KeyError:
            entity.born = None
            entity.born_precision = None

        try:
            death_date = self._get_date_and_precision(
                info['d_year'], info['d_month'], info['d_day']
            )
            entity.died = death_date[0]
            entity.died_precision = death_date[1]
        except KeyError:
            entity.died = None
            entity.died_precision = None

        if isinstance(entity, (MusicbrainzArtistEntity, MusicbrainzBandEntity)):
            try:
                entity.birth_place = areas[info['b_place']]
            except KeyError:
                entity.birth_place = None
            try:
                entity.death_place = areas[info['d_place']]
            except KeyError:
                entity.death_place = None

    @staticmethod
    def _fill_link_entity(entity, gid, link):
        entity.catalog_id = gid
        entity.url = link
        entity.is_wiki = url_utils.is_wiki_link(link)
        url_tokens = url_utils.tokenize(link)
        if url_tokens:
            entity.url_tokens = ' '.join(url_tokens)

    @staticmethod
    def _alias_entities(entity: BaseEntity, aliases_class, aliases: []):
        for alias_label in aliases:
            alias_entity = aliases_class()
            alias_entity.catalog_id = entity.catalog_id
            alias_entity.born = entity.born
            alias_entity.born_precision = entity.born_precision
            alias_entity.died = entity.died
            alias_entity.died_precision = entity.died_precision
            alias_entity.birth_place = entity.birth_place
            alias_entity.death_place = entity.death_place

            alias_entity.name = alias_label
            name_tokens = text_utils.tokenize(alias_label)
            if name_tokens:
                alias_entity.name_tokens = ' '.join(name_tokens)
            yield alias_entity

    @staticmethod
    def _get_date_and_precision(year, month, day):
        date_list = [year, month, day]
        precision = -1

        try:
            if date_list[0] != '\\N' and int(date_list[0]) < 0:
                LOGGER.warning(
                    'Failed to convert date (%s/%s/%s).'
                    'Encountered negative year, '
                    'which Python Date object does not support',
                    *date_list,
                )

                # We can't parse the date,
                # so we treat is as if it wasn't available
                date_list[0] = '\\N'

            null_index = date_list.index('\\N')
            precision = 8 + null_index if null_index > 0 else -1

        except ValueError:
            precision = 11

        date_list = ['0001' if i == '\\N' else i for i in date_list]

        if precision == -1:
            return None, None

        return (
            date(int(date_list[0]), int(date_list[1]), int(date_list[2])),
            precision,
        )

    @staticmethod
    def _check_person(type_code):
        # person, character
        return type_code in ['1', '4']

    @staticmethod
    def _check_band(type_code):
        # group, orchestra, choir
        return type_code in ['2', '5', '6']

    @staticmethod
    def _artist_gender(gender_code):
        genders = {'1': 'male', '2': 'female'}
        return genders.get(gender_code, None)

    def _retrieve_release_group_dates(self, dump_path):
        release_dateprec = defaultdict(lambda: (date.today(), 0))

        release_country_path = os.path.join(
            dump_path, 'mbdump', 'release_country'
        )

        with open(release_country_path) as rfile:
            releases = DictReader(
                rfile,
                delimiter='\t',
                fieldnames=['release_id', 'country_id', 'year', 'month', 'day'],
            )

            for release in releases:
                date_prec = self._get_date_and_precision(
                    release['year'], release['month'], release['day']
                )

                if date_prec[0] is None:
                    continue

                if date_prec[0] < release_dateprec[release['release_id']][0]:
                    release_dateprec[release['release_id']] = date_prec

        release_country_path = os.path.join(
            dump_path, 'mbdump', 'release_unknown_country'
        )

        with open(release_country_path) as rfile:
            releases = DictReader(
                rfile,
                delimiter='\t',
                fieldnames=['release_id', 'year', 'month', 'day'],
            )

            for release in releases:
                date_prec = self._get_date_and_precision(
                    release['year'], release['month'], release['day']
                )

                if date_prec[0] is None:
                    continue

                if date_prec[0] < release_dateprec[release['release_id']][0]:
                    release_dateprec[release['release_id']] = date_prec

        release_group_dateprec = defaultdict(lambda: (date.today(), 0))
        release_path = os.path.join(dump_path, 'mbdump', 'release')
        with open(release_path) as rfile:
            releases = DictReader(
                rfile,
                delimiter='\t',
                fieldnames=[
                    'release_id',
                    'gid',
                    'name',
                    'credits',
                    'release_group_id',
                ],
            )

            for release in releases:
                if (
                    release_dateprec[release['release_id']][0]
                    < release_group_dateprec[release['release_group_id']][0]
                ):
                    release_group_dateprec[
                        release['release_group_id']
                    ] = release_dateprec[release['release_id']]

        return release_group_dateprec
