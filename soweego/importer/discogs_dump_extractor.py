#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Discogs dump extractor"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import logging
import xml.etree.ElementTree as et
from datetime import date, datetime

from requests import get

from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import discogs_entity
from soweego.importer.models.base_link_entity import BaseLinkEntity

LOGGER = logging.getLogger(__name__)

DUMP_BASE_URL = 'https://discogs-data.s3-us-west-2.amazonaws.com/'
DUMP_LIST_URL_TEMPLATE = DUMP_BASE_URL + '?delimiter=/&prefix=data/{}/'


class DiscogsDumpExtractor(BaseDumpExtractor):

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
        response = get(DUMP_LIST_URL_TEMPLATE.format(date.today().year))
        root = et.fromstring(response.text)
        # 4 dump files, sorted alphabetically: artists, labels, masters, releases
        latest = list(root)[-4]  # Take the 4th from last child
        for child in latest:
            if 'Key' in child.tag:
                dump_file_name = child.text
        return DUMP_BASE_URL + dump_file_name

    def extract_and_populate(self, dump_file_path: str):
        LOGGER.info(
            "Starting import of musicians and bands from Discogs dump '%s'", dump_file_path)
        start = datetime.now()

        tables = [discogs_entity.DiscogsMusicianEntity, discogs_entity.DiscogsMusicianNlpEntity, discogs_entity.DiscogsMusicianLinkEntity,
                  discogs_entity.DiscogsGroupEntity, discogs_entity.DiscogsGroupNlpEntity, discogs_entity.DiscogsGroupLinkEntity]

        db_manager = DBManager()
        LOGGER.info('Connected to database: %s', db_manager.get_engine().url)

        db_manager.drop(tables)
        db_manager.create(tables)
        LOGGER.info('SQL tables dropped and re-created: %s',
                    [table.__tablename__ for table in tables])

        with gzip.open(dump_file_path, 'rt') as dump:
            for _, node in et.iterparse(dump):
                if not node.tag == 'artist':
                    continue

                # Skip nodes without required fields
                identifier = node.findtext('id')
                if not identifier:
                    LOGGER.warning(
                        'Skipping import for artist node with no identifier: %s', node)
                    continue
                name = node.findtext('name')
                if not name:
                    LOGGER.warning(
                        'Skipping import for identifier with no name: %s', identifier)
                    continue

                living_links = self._extract_living_links(node, identifier)

                session = db_manager.new_session()

                # Musician
                groups = node.find('groups')
                members = node.find('members')
                if groups:
                    entity = discogs_entity.DiscogsMusicianEntity()
                    self._populate_musician(
                        entity, identifier, name, living_links, node, session)
                # Band
                elif members:
                    entity = discogs_entity.DiscogsGroupEntity()
                    self._populate_band(entity, identifier,
                                        name, living_links, node, session)
                # Can't infer the entity type, so populate both
                else:
                    LOGGER.debug(
                        'Unknown artist type. Will add it to both musicians and bands: %s', identifier)
                    entity = discogs_entity.DiscogsMusicianEntity()
                    self._populate_musician(
                        entity, identifier, name, living_links, node, session)
                    entity = discogs_entity.DiscogsGroupEntity()
                    self._populate_band(entity, identifier,
                                        name, living_links, node, session)

                session.commit()
                LOGGER.debug('%d entities imported so far: %d musicians with %d links, %d bands with %d links, %d discarded dead links.',
                             self.total_entities, self.musicians, self.musician_links, self.bands, self.band_links, self.dead_links)

        end = datetime.now()
        LOGGER.info('Import completed in %d. Total entities: %d - %d musicians with %d links - %d bands with %d links - %d discarded dead links.',
                    end - start, self.total_entities, self.musicians, self.musician_links, self.bands, self.band_links, self.dead_links)

    def _populate_band(self, entity: discogs_entity.DiscogsGroupEntity, identifier, name, links, node, session):
        # Main entity
        self._fill_entity(entity, identifier, name, node)
        session.add(entity)
        self.bands += 1
        self.total_entities += 1
        # Textual data
        self._populate_nlp_entity(
            session, node, discogs_entity.DiscogsGroupNlpEntity, identifier)
        # Denormalized name variations
        self._populate_name_variations(session, node, entity, identifier)
        # Links
        for link in links:
            self._fill_link_entity(
                discogs_entity.DiscogsMusicianLinkEntity(), identifier, link)
        # TODO populate group -> musicians relationship table
        #  for member in list(members):
        #      get member.attrib['id']

    def _populate_musician(self, entity: discogs_entity.DiscogsMusicianEntity, identifier, name, links, node, session):
        # Main entity
        self._fill_entity(entity, identifier, name, node)
        session.add(entity)
        self.musicians += 1
        self.total_entities += 1
        # Textual data
        self._populate_nlp_entity(
            session, node, discogs_entity.DiscogsMusicianNlpEntity, identifier)
        # Denormalized name variations
        self._populate_name_variations(session, node, entity, identifier)
        # Links
        for link in links:
            self._fill_link_entity(
                discogs_entity.DiscogsMusicianLinkEntity(), identifier, link)
        # TODO populate musician -> groups relationship table
        #  for group in list(groups):
        #      get group.attrib['id']

    def _populate_name_variations(self, session, artist_node, current_entity, identifier):
        name_variations_node = artist_node.find('namevariations')
        if name_variations_node:
            children = list(name_variations_node)
            if children:
                session.add_all(self._denormalize_name_variation_entities(
                    current_entity, children))
            else:
                LOGGER.debug(
                    'Artist %s has an empty <namevariations/> tag', identifier)
        else:
            LOGGER.debug(
                'Artist %s has no <namevariations> tag', identifier)

    def _populate_nlp_entity(self, session, artist_node, entity_class, identifier):
        profile = artist_node.findtext('profile')
        if profile:
            nlp_entity = entity_class()
            nlp_entity.catalog_id = identifier
            nlp_entity.description = profile
            nlp_entity.tokens = ' '.join(text_utils.tokenize(profile))
            session.add(nlp_entity)
            self.total_entities += 1
            if 'Musician' in entity_class.__name__:
                self.musician_nlp += 1
            else:
                self.band_nlp += 1
        else:
            LOGGER.debug('Artist %s has an empty <profile/> tag', identifier)

    def _fill_entity(self, entity: discogs_entity.DiscogsBaseEntity, identifier, name, artist_node):
        # Required fields
        entity.catalog_id = identifier
        entity.name = name
        # Real name
        real_name = artist_node.findtext('realname')
        if real_name:
            entity.real_name = real_name
        else:
            LOGGER.debug(
                'Artist %s has an empty <realname/> tag', identifier)
        # Data quality
        data_quality = artist_node.findtext('data_quality')
        if data_quality:
            entity.data_quality = data_quality
        else:
            LOGGER.debug(
                'Artist %s has an empty <data_quality/> tag', identifier)

    def _denormalize_name_variation_entities(self, main_entity: discogs_entity.DiscogsBaseEntity, name_variation_nodes):
        entity_class = type(main_entity)
        for node in name_variation_nodes:
            name_variation = node.text
            if not name_variation:
                LOGGER.debug(
                    'Artist %s: skipping empty <name> tag in <namevariations>', main_entity.catalog_id)
                continue
            variation_entity = entity_class()
            variation_entity.catalog_id = main_entity.catalog_id
            variation_entity.name = name_variation
            variation_entity.real_name = main_entity.real_name
            variation_entity.data_quality = main_entity.data_quality
            self.total_entities += 1
            if 'Musician' in entity_class.__name__:
                self.musicians += 1
            else:
                self.bands += 1
            yield variation_entity

    def _extract_living_links(self, artist_node, identifier):
        LOGGER.debug('Extracting living links from artist %s', identifier)
        urls = artist_node.find('urls')
        if urls:
            for url_element in urls.iterfind('url'):
                url = url_element.text
                if not url:
                    LOGGER.debug(
                        'Artist %s: skipping empty <url> tag', identifier)
                    continue
                for alive_link in self._check_link(url):
                    yield alive_link

    def _check_link(self, link):
        LOGGER.debug('Processing link <%s>', link)
        clean_parts = url_utils.clean(link)
        LOGGER.debug('Clean link: %s', clean_parts)
        for part in clean_parts:
            valid = url_utils.validate(part)
            if not valid:
                self.dead_links += 1
                continue
            LOGGER.debug('Valid URL: <%s>', valid)
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
        entity.tokens = '|'.join(url_utils.tokenize(url))
        if isinstance(entity, discogs_entity.DiscogsMusicianLinkEntity):
            self.musician_links += 1
        elif isinstance(entity, discogs_entity.DiscogsGroupLinkEntity):
            self.band_links += 1
