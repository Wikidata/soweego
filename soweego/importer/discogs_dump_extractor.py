#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`Discogs <https://www.discogs.com/>`_ dump extractor."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import logging
import os
import shutil
import xml.etree.ElementTree as et
from datetime import date, datetime
from typing import Iterable, List, Optional, Tuple

from lxml import etree
from requests import get
from tqdm import tqdm

from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.discogs_entity import (
    DiscogsBaseEntity,
    DiscogsArtistEntity,
    DiscogsGroupEntity,
    DiscogsGroupLinkEntity,
    DiscogsGroupNlpEntity,
    DiscogsMasterArtistRelationship,
    DiscogsMasterEntity,
    DiscogsMusicianEntity,
    DiscogsMusicianLinkEntity,
    DiscogsMusicianNlpEntity,
)

LOGGER = logging.getLogger(__name__)

DUMP_BASE_URL = 'https://discogs-data.s3-us-west-2.amazonaws.com/'
DUMP_LIST_URL_TEMPLATE = DUMP_BASE_URL + '?delimiter=/&prefix=data/{}/'


class DiscogsDumpExtractor(BaseDumpExtractor):
    """Download Discogs dumps, extract data, and
    populate a database instance."""

    # Counters
    total_entities = 0
    musicians = 0
    musician_links = 0
    musician_nlp = 0
    bands = 0
    band_links = 0
    band_nlp = 0
    artists = 0
    valid_links = 0
    dead_links = 0

    _sqlalchemy_commit_every = 100_000

    def get_dump_download_urls(self) -> Optional[List[str]]:
        urls = []
        response = get(DUMP_LIST_URL_TEMPLATE.format(date.today().year))
        root = et.fromstring(response.text)
        # 4 dump files, sorted alphabetically: artists, labels, masters,
        # releases
        dumps = [
            list(root)[-4],
            list(root)[-2],
        ]  # Take the 2nd and 4th from last child
        for dump in dumps:
            for child in dump:
                if 'Key' in child.tag:
                    urls.append(DUMP_BASE_URL + child.text)
                    break
        if not urls:
            LOGGER.error(
                'Failed to get the Discogs dump download URL: are we at the '
                'very start of the year?'
            )
            return None
        return urls

    def extract_and_populate(self, dump_file_paths: List[str], resolve: bool) -> None:
        """Extract relevant data from the *artists* (people)
        and *masters* (works) Discogs dumps, preprocess them, populate
        `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities, and persist
        them to a database instance.

        See :mod:`~soweego.importer.models.discogs_entity`
        for the ORM definitions.

        :param dump_file_paths: paths to downloaded catalog dumps
        :param resolve: whether to resolve URLs found in catalog dumps or not
        """
        self._process_artists_dump(dump_file_paths[0], resolve)
        self._process_masters_dump(dump_file_paths[1])

    def _process_masters_dump(self, dump_file_path):
        LOGGER.info("Starting import of masters from Discogs dump '%s'", dump_file_path)
        start = datetime.now()
        tables = [DiscogsMasterEntity, DiscogsMasterArtistRelationship]
        db_manager = DBManager()
        LOGGER.info('Connected to database: %s', db_manager.get_engine().url)
        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info(
            'SQL tables dropped and re-created: %s',
            [table.__tablename__ for table in tables],
        )
        extracted_path = '.'.join(dump_file_path.split('.')[:-1])
        # Extract dump file if it has not yet been extracted
        if not os.path.exists(extracted_path):
            LOGGER.info('Extracting dump file')

            with gzip.open(dump_file_path, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        # count number of entries
        n_rows = sum(1 for _ in self._g_process_et_items(extracted_path, 'master'))
        session = db_manager.new_session()
        entity_array = []  # array to which we'll add the entities
        relationships_set = set()
        self.total_entities = 0
        for _, node in tqdm(
            self._g_process_et_items(extracted_path, 'master'), total=n_rows
        ):

            if not node.tag == 'master':
                continue

            self.total_entities += 1
            entity = self._extract_from_master_node(node, relationships_set)
            entity_array.append(entity)
            # commit in batches of `self._sqlalchemy_commit_every`
            if len(entity_array) >= self._sqlalchemy_commit_every:
                LOGGER.info(
                    'Adding batch of entities to the database, '
                    'this will take a while.'
                    'Progress will resume soon.'
                )

                insert_start_time = datetime.now()

                session.bulk_save_objects(entity_array)
                session.commit()
                session.expunge_all()  # clear session

                entity_array.clear()  # clear entity array

                LOGGER.debug(
                    'It took %s to add %s entities to the database',
                    datetime.now() - insert_start_time,
                    self._sqlalchemy_commit_every,
                )
        # finally commit remaining entities in session
        # (if any), and close session
        session.bulk_save_objects(entity_array)
        session.bulk_save_objects(
            [
                DiscogsMasterArtistRelationship(id1, id2)
                for id1, id2 in relationships_set
            ]
        )
        session.commit()
        session.close()

        end = datetime.now()
        LOGGER.info(
            'Import completed in %s. Total entities: %d. ' 'Total relationships %s.',
            end - start,
            self.total_entities,
            len(relationships_set),
        )
        # once the import process is complete,
        # we can safely delete the extracted discogs dump
        os.remove(extracted_path)

    @staticmethod
    def _extract_from_master_node(node, relationships_set):
        entity = DiscogsMasterEntity()
        entity.catalog_id = node.attrib['id']
        genres = set()
        for child in node:
            if child.tag == 'main_release':
                entity.main_release_id = child.text
            elif child.tag == 'genres':
                for genre in child:
                    genres.update(text_utils.tokenize(genre.text))
            elif child.tag == 'styles':
                for style in child:
                    genres.update(text_utils.tokenize(style.text))
            elif child.tag == 'title':
                entity.name = child.text
                entity.name_tokens = ' '.join(text_utils.tokenize(child.text))
            elif child.tag == 'data_quality':
                entity.data_quality = child.text.lower()
            elif child.tag == 'year':
                try:
                    entity.born = date(year=int(child.text), month=1, day=1)
                    entity.born_precision = 9
                except ValueError:
                    LOGGER.debug(
                        'Master with id %s has an invalid year: %s',
                        entity.catalog_id,
                        child.text,
                    )
            elif child.tag == 'artists':
                for artist in child:
                    relationships_set.add((entity.catalog_id, artist.find('id').text))
        entity.genres = ' '.join(genres)
        return entity

    def _extract_from_artist_node(self, node, resolve: bool) -> dict:
        infos = {}
        # Skip nodes without required fields
        identifier = node.findtext('id')
        if not identifier:
            LOGGER.warning(
                'Skipping import for artist node with no identifier: %s', node
            )
            return None

        name = node.findtext('name')
        if not name:
            LOGGER.warning(
                'Skipping import for identifier with no name: %s', identifier
            )
            return None

        infos['identifier'] = identifier
        infos['name'] = name

        # Musician
        groups = node.find('groups')
        members = node.find('members')

        if groups is not None:
            infos['groups'] = groups
        if members is not None:
            infos['members'] = members

        infos['realname'] = node.findtext('realname')
        infos['data_quality'] = node.findtext('data_quality')
        infos['profile'] = node.findtext('profile')
        infos['namevariations'] = node.find('namevariations')

        infos['living_links'] = self._extract_living_links(identifier, node, resolve)

        return infos

    def _process_artists_dump(self, dump_file_path, resolve):
        LOGGER.info(
            "Starting import of musicians and bands from Discogs dump '%s'",
            dump_file_path,
        )
        start = datetime.now()
        tables = [
            DiscogsMusicianEntity,
            DiscogsMusicianNlpEntity,
            DiscogsMusicianLinkEntity,
            DiscogsGroupEntity,
            DiscogsGroupNlpEntity,
            DiscogsGroupLinkEntity,
            DiscogsArtistEntity,
        ]
        db_manager = DBManager()
        LOGGER.info('Connected to database: %s', db_manager.get_engine().url)
        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info(
            'SQL tables dropped and re-created: %s',
            [table.__tablename__ for table in tables],
        )
        extracted_path = '.'.join(dump_file_path.split('.')[:-1])
        # Extract dump file if it has not yet been extracted
        if not os.path.exists(extracted_path):
            LOGGER.info('Extracting dump file ...')

            with gzip.open(dump_file_path, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        # count number of entries
        n_rows = sum(1 for _ in self._g_process_et_items(extracted_path, 'artist'))
        session = db_manager.new_session()
        entity_array = []  # array to which we'll add the entities
        for _, node in tqdm(
            self._g_process_et_items(extracted_path, 'artist'), total=n_rows
        ):

            if not node.tag == 'artist':
                continue

            infos = self._extract_from_artist_node(node, resolve)

            if infos is None:
                continue

            # NOTE Heuristic to distinguish between musicians and bands:
            #      <groups> tag = musician
            #      <members> tag = band
            #      Otherwise a generic artist
            if 'groups' in infos:                                   # Musician
                entity = DiscogsMusicianEntity()
                self._populate_musician(entity_array, entity, infos)
            elif 'members' in infos:                                # Band
                entity = DiscogsGroupEntity()
                self._populate_band(entity_array, entity, infos)
            else:                                                   # Generic
                LOGGER.debug('Generic artist: %s', infos['identifier'])
                entity = DiscogsArtistEntity()
                self._fill_entity(entity, infos)
                self.artists += 1
                self.total_entities += 1
                entity_array.append(entity)

            # commit in batches of `self._sqlalchemy_commit_every`
            if len(entity_array) >= self._sqlalchemy_commit_every:
                LOGGER.info(
                    'Adding batch of entities to the database, '
                    'this will take a while. '
                    'Progress will resume soon.'
                )

                insert_start_time = datetime.now()

                session.bulk_save_objects(entity_array)
                session.commit()
                session.expunge_all()  # clear session

                entity_array.clear()  # clear entity array

                LOGGER.debug(
                    'It took %s to add %s entities to the database',
                    datetime.now() - insert_start_time,
                    self._sqlalchemy_commit_every,
                )
        # finally commit remaining entities in session
        # (if any), and close session
        session.bulk_save_objects(entity_array)
        session.commit()
        session.close()
        end = datetime.now()
        LOGGER.info(
            'Import completed in %s. '
            'Total entities: %d - %d musicians with %d links - %d bands'
            ' with %d links - %d discarded dead links.',
            end - start,
            self.total_entities,
            self.musicians,
            self.musician_links,
            self.bands,
            self.band_links,
            self.dead_links,
        )
        # once the import process is complete,
        # we can safely delete the extracted discogs dump
        os.remove(extracted_path)

    def _populate_band(self, entity_array, entity: DiscogsGroupEntity, infos: dict):
        # Main entity
        self._fill_entity(entity, infos)
        self.bands += 1
        self.total_entities += 1
        # Textual data
        self._populate_nlp_entity(entity_array, infos, DiscogsGroupNlpEntity)
        # Denormalized name variations
        self._populate_name_variations(entity_array, infos, entity)
        # Links
        self._populate_links(entity_array, DiscogsGroupLinkEntity, infos)

        entity_array.append(entity)

        # TODO populate group -> musicians relationship table
        #  for member in list(members):
        #      get member.attrib['id']

    def _populate_musician(
        self, entity_array, entity: DiscogsMusicianEntity, infos: dict
    ):
        # Main entity
        self._fill_entity(entity, infos)
        self.musicians += 1
        self.total_entities += 1
        # Textual data
        self._populate_nlp_entity(entity_array, infos, DiscogsMusicianNlpEntity)
        # Denormalized name variations
        self._populate_name_variations(entity_array, infos, entity)
        # Links
        self._populate_links(entity_array, DiscogsMusicianLinkEntity, infos)

        entity_array.append(entity)

        # TODO populate musician -> groups relationship table
        #  for group in list(groups):
        #      get group.attrib['id']

    def _populate_links(self, entity_array, entity_class, infos: dict):
        for link in infos['living_links']:
            link_entity = entity_class()
            self._fill_link_entity(link_entity, infos['identifier'], link)
            entity_array.append(link_entity)

    def _populate_name_variations(self, entity_array, infos: dict, current_entity):
        identifier = infos['identifier']
        if infos.get('namevariations') is not None:
            children = list(infos['namevariations'])
            if children:
                for entity in self._denormalize_name_variation_entities(
                    current_entity, children
                ):
                    entity_array.append(entity)
            else:
                LOGGER.debug('Artist %s has an empty <namevariations/> tag', identifier)
        else:
            LOGGER.debug('Artist %s has no <namevariations> tag', identifier)

    def _populate_nlp_entity(self, entity_array, infos: dict, entity_class):
        if infos.get('profile'):
            nlp_entity = entity_class()
            nlp_entity.catalog_id = infos['identifier']
            nlp_entity.description = infos['profile']
            description_tokens = text_utils.tokenize(infos['profile'])
            if description_tokens:
                nlp_entity.description_tokens = ' '.join(description_tokens)
            entity_array.append(nlp_entity)
            self.total_entities += 1
            if 'Musician' in entity_class.__name__:
                self.musician_nlp += 1
            else:
                self.band_nlp += 1
        else:
            LOGGER.debug('Artist %s has an empty <profile/> tag', infos['identifier'])

    @staticmethod
    def _fill_entity(entity: DiscogsBaseEntity, infos):
        # Base fields
        entity.catalog_id = infos['identifier']
        entity.name = infos['name']
        name_tokens = text_utils.tokenize(infos['name'])
        if name_tokens:
            entity.name_tokens = ' '.join(name_tokens)
        # Real name
        real_name = infos['realname']
        if real_name:
            entity.real_name = real_name
        else:
            LOGGER.debug('Artist %s has an empty <realname/> tag', infos['identifier'])
        # Data quality
        data_quality = infos['data_quality']
        if data_quality:
            entity.data_quality = data_quality
        else:
            LOGGER.debug(
                'Artist %s has an empty <data_quality/> tag',
                infos['identifier'],
            )

    def _denormalize_name_variation_entities(
        self, main_entity: DiscogsBaseEntity, name_variation_nodes
    ):
        entity_class = type(main_entity)
        for node in name_variation_nodes:
            name_variation = node.text
            if not name_variation:
                LOGGER.debug(
                    'Artist %s: skipping empty <name> tag in <namevariations>',
                    main_entity.catalog_id,
                )
                continue
            variation_entity = entity_class()
            variation_entity.catalog_id = main_entity.catalog_id
            variation_entity.name = name_variation
            name_tokens = text_utils.tokenize(name_variation)
            if name_tokens:
                variation_entity.name_tokens = ' '.join(name_tokens)
            variation_entity.real_name = main_entity.real_name
            variation_entity.data_quality = main_entity.data_quality
            self.total_entities += 1
            if 'Musician' in entity_class.__name__:
                self.musicians += 1
            else:
                self.bands += 1
            yield variation_entity

    def _extract_living_links(self, identifier, node, resolve: bool):
        LOGGER.debug('Extracting living links from artist %s', identifier)
        urls = node.find('urls')
        if urls is not None:
            for url_element in urls.iterfind('url'):
                url = url_element.text
                if not url:
                    LOGGER.debug('Artist %s: skipping empty <url> tag', identifier)
                    continue
                for alive_link in self._check_link(url, resolve):
                    yield alive_link

    def _check_link(self, link, resolve: bool):
        LOGGER.debug('Processing link <%s>', link)
        clean_parts = url_utils.clean(link)
        LOGGER.debug('Clean link: %s', clean_parts)
        for part in clean_parts:
            valid = url_utils.validate(part)
            if not valid:
                self.dead_links += 1
                continue
            LOGGER.debug('Valid URL: <%s>', valid)
            if not resolve:
                yield valid
                continue
            alive = url_utils.resolve(valid)
            if not alive:
                self.dead_links += 1
                continue
            LOGGER.debug('Living URL: <%s>', alive)
            self.valid_links += 1
            yield alive

    def _fill_link_entity(self, entity: BaseLinkEntity, identifier, url):
        entity.catalog_id = identifier
        entity.url = url
        entity.is_wiki = url_utils.is_wiki_link(url)
        entity.url_tokens = ' '.join(url_utils.tokenize(url))
        if isinstance(entity, DiscogsMusicianLinkEntity):
            self.musician_links += 1
        elif isinstance(entity, DiscogsGroupLinkEntity):
            self.band_links += 1

    @staticmethod
    def _g_process_et_items(path, tag) -> Iterable[Tuple]:
        """
        Generator: Processes ElementTree items in a memory
        efficient way
        """

        context: etree.ElementTree = etree.iterparse(path, events=('end',), tag=tag)

        for event, elem in context:
            yield event, elem

            # delete content of node once we're done processing
            # it. If we don't then it would stay in memory
            elem.clear()
