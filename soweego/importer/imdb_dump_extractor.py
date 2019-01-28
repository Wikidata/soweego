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
    total_entities = 0
    musicians = 0
    musician_links = 0
    musician_nlp = 0
    bands = 0
    band_links = 0
    band_nlp = 0
    valid_links = 0
    dead_links = 0

    def get_dump_download_url(self) -> str:
        return DUMP_URL_PERSON_INFO

    def _download_movies_dataset(self, dump_file_path: str):
        
        filename = DUMP_URL_MOVIE_INFO.split("/")[-1]

        # the files get updated once each day, so we downloa the file for today
        filename = datetime.date.today().strftime('%d-%m-%Y-') + filename

        path_to_download = "/".join(dump_file_path.split("/")[:-1]) + "/" + filename

        # Check if the current dump is up-to-date
        if not os.path.isfile(path_to_download):
            client.download_file(DUMP_URL_MOVIE_INFO, path_to_download)

        return path_to_download

    def extract_and_populate(self, dump_file_path: str):

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
        LOGGER.info(f"Connected to database: {db_manager.get_engine().url}")

        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info(
            f"SQL tables dropped and re-created: {[table.__tablename__ for table in tables]}")
        LOGGER.info("Downloading movie dataset")

        movies_file_path = self._download_movies_dataset(dump_file_path)

        LOGGER.info(
            f"Movie dataset has been downloaded to '{movies_file_path}'")

        LOGGER.info("Starting to import movies from imdb dump")

        with gzip.open(movies_file_path, "rt") as mdump:
            reader = csv.DictReader(mdump, delimiter="\t")
            for movie in reader:
                print(movie)
                raise Exception

        LOGGER.info(
            f"Starting import persons from IMDB dump '{dump_file_path}'")

        with gzip.open(dump_file_path, "rt") as pdump:
            reader = csv.DictReader(pdump, delimiter="\t")

            for person in reader:
                print(person)
                raise Exception

    #     with gzip.open(dump_file_path, 'rt') as dump:
    #         for _, node in et.iterparse(dump):
    #             if not node.tag == 'artist':
    #                 continue

    #             # Skip nodes without required fields
    #             identifier = node.findtext('id')
    #             if not identifier:
    #                 LOGGER.warning(
    #                     'Skipping import for artist node with no identifier: %s', node)
    #                 continue
    #             name = node.findtext('name')
    #             if not name:
    #                 LOGGER.warning(
    #                     'Skipping import for identifier with no name: %s', identifier)
    #                 continue

    #             living_links = self._extract_living_links(node, identifier)

    #             session = db_manager.new_session()

    #             # Musician
    #             groups = node.find('groups')
    #             members = node.find('members')
    #             if groups:
    #                 entity = discogs_entity.DiscogsMusicianEntity()
    #                 self._populate_musician(
    #                     entity, identifier, name, living_links, node, session)
    #             # Band
    #             elif members:
    #                 entity = discogs_entity.DiscogsGroupEntity()
    #                 self._populate_band(entity, identifier,
    #                                     name, living_links, node, session)
    #             # Can't infer the entity type, so populate both
    #             else:
    #                 LOGGER.debug(
    #                     'Unknown artist type. Will add it to both musicians and bands: %s', identifier)
    #                 entity = discogs_entity.DiscogsMusicianEntity()
    #                 self._populate_musician(
    #                     entity, identifier, name, living_links, node, session)
    #                 entity = discogs_entity.DiscogsGroupEntity()
    #                 self._populate_band(entity, identifier,
    #                                     name, living_links, node, session)

    #             session.commit()
    #             LOGGER.debug('%d entities imported so far: %d musicians with %d links, %d bands with %d links, %d discarded dead links.',
    #                          self.total_entities, self.musicians, self.musician_links, self.bands, self.band_links, self.dead_links)

    #     end = datetime.now()
    #     LOGGER.info('Import completed in %s. Total entities: %d - %d musicians with %d links - %d bands with %d links - %d discarded dead links.',
    #                 end - start, self.total_entities, self.musicians, self.musician_links, self.bands, self.band_links, self.dead_links)

    # def _populate_band(self, entity: discogs_entity.DiscogsGroupEntity, identifier, name, links, node, session):
    #     # Main entity
    #     self._fill_entity(entity, identifier, name, node)
    #     session.add(entity)
    #     self.bands += 1
    #     self.total_entities += 1
    #     # Textual data
    #     self._populate_nlp_entity(
    #         session, node, discogs_entity.DiscogsGroupNlpEntity, identifier)
    #     # Denormalized name variations
    #     self._populate_name_variations(session, node, entity, identifier)
    #     # Links
    #     self._populate_links(
    #         session, links, discogs_entity.DiscogsGroupLinkEntity, identifier)
    #     # TODO populate group -> musicians relationship table
    #     #  for member in list(members):
    #     #      get member.attrib['id']

    # def _populate_musician(self, entity: discogs_entity.DiscogsMusicianEntity, identifier, name, links, node, session):
    #     # Main entity
    #     self._fill_entity(entity, identifier, name, node)
    #     session.add(entity)
    #     self.musicians += 1
    #     self.total_entities += 1
    #     # Textual data
    #     self._populate_nlp_entity(
    #         session, node, discogs_entity.DiscogsMusicianNlpEntity, identifier)
    #     # Denormalized name variations
    #     self._populate_name_variations(session, node, entity, identifier)
    #     # Links
    #     self._populate_links(
    #         session, links, discogs_entity.DiscogsMusicianLinkEntity, identifier)
    #     # TODO populate musician -> groups relationship table
    #     #  for group in list(groups):
    #     #      get group.attrib['id']

    # def _populate_links(self, session, links, entity_class, identifier):
    #     for link in links:
    #         link_entity = entity_class()
    #         self._fill_link_entity(link_entity, identifier, link)
    #         session.add(link_entity)

    # def _populate_name_variations(self, session, artist_node, current_entity, identifier):
    #     name_variations_node = artist_node.find('namevariations')
    #     if name_variations_node:
    #         children = list(name_variations_node)
    #         if children:
    #             session.add_all(self._denormalize_name_variation_entities(
    #                 current_entity, children))
    #         else:
    #             LOGGER.debug(
    #                 'Artist %s has an empty <namevariations/> tag', identifier)
    #     else:
    #         LOGGER.debug(
    #             'Artist %s has no <namevariations> tag', identifier)

    # def _populate_nlp_entity(self, session, artist_node, entity_class, identifier):
    #     profile = artist_node.findtext('profile')
    #     if profile:
    #         nlp_entity = entity_class()
    #         nlp_entity.catalog_id = identifier
    #         nlp_entity.description = profile
    #         nlp_entity.tokens = ' '.join(text_utils.tokenize(profile))
    #         session.add(nlp_entity)
    #         self.total_entities += 1
    #         if 'Musician' in entity_class.__name__:
    #             self.musician_nlp += 1
    #         else:
    #             self.band_nlp += 1
    #     else:
    #         LOGGER.debug('Artist %s has an empty <profile/> tag', identifier)

    # def _fill_entity(self, entity: discogs_entity.DiscogsBaseEntity, identifier, name, artist_node):
    #     # Required fields
    #     entity.catalog_id = identifier
    #     entity.name = name
    #     entity.tokens = ' '.join(text_utils.tokenize(name))
    #     # Real name
    #     real_name = artist_node.findtext('realname')
    #     if real_name:
    #         entity.real_name = real_name
    #     else:
    #         LOGGER.debug(
    #             'Artist %s has an empty <realname/> tag', identifier)
    #     # Data quality
    #     data_quality = artist_node.findtext('data_quality')
    #     if data_quality:
    #         entity.data_quality = data_quality
    #     else:
    #         LOGGER.debug(
    #             'Artist %s has an empty <data_quality/> tag', identifier)

    # def _denormalize_name_variation_entities(self, main_entity: discogs_entity.DiscogsBaseEntity, name_variation_nodes):
    #     entity_class = type(main_entity)
    #     for node in name_variation_nodes:
    #         name_variation = node.text
    #         if not name_variation:
    #             LOGGER.debug(
    #                 'Artist %s: skipping empty <name> tag in <namevariations>', main_entity.catalog_id)
    #             continue
    #         variation_entity = entity_class()
    #         variation_entity.catalog_id = main_entity.catalog_id
    #         variation_entity.name = name_variation
    #         variation_entity.tokens = ' '.join(
    #             text_utils.tokenize(name_variation))
    #         variation_entity.real_name = main_entity.real_name
    #         variation_entity.data_quality = main_entity.data_quality
    #         self.total_entities += 1
    #         if 'Musician' in entity_class.__name__:
    #             self.musicians += 1
    #         else:
    #             self.bands += 1
    #         yield variation_entity

    # def _extract_living_links(self, artist_node, identifier):
    #     LOGGER.debug('Extracting living links from artist %s', identifier)
    #     urls = artist_node.find('urls')
    #     if urls:
    #         for url_element in urls.iterfind('url'):
    #             url = url_element.text
    #             if not url:
    #                 LOGGER.debug(
    #                     'Artist %s: skipping empty <url> tag', identifier)
    #                 continue
    #             for alive_link in self._check_link(url):
    #                 yield alive_link

    # def _check_link(self, link):
    #     LOGGER.debug('Processing link <%s>', link)
    #     clean_parts = url_utils.clean(link)
    #     LOGGER.debug('Clean link: %s', clean_parts)
    #     for part in clean_parts:
    #         valid = url_utils.validate(part)
    #         if not valid:
    #             self.dead_links += 1
    #             continue
    #         LOGGER.debug('Valid URL: <%s>', valid)
    #         alive = url_utils.resolve(valid)
    #         if not alive:
    #             self.dead_links += 1
    #             continue
    #         LOGGER.debug('Living URL: <%s>', alive)
    #         self.valid_links += 1
    #         yield alive

    # def _fill_link_entity(self, entity: BaseLinkEntity, identifier, url):
    #     entity.catalog_id = identifier
    #     entity.url = url
    #     entity.is_wiki = url_utils.is_wiki_link(url)
    #     entity.tokens = '|'.join(url_utils.tokenize(url))
    #     if isinstance(entity, discogs_entity.DiscogsMusicianLinkEntity):
    #         self.musician_links += 1
    #     elif isinstance(entity, discogs_entity.DiscogsGroupLinkEntity):
    #         self.band_links += 1
