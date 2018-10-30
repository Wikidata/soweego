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
from urllib.parse import urlsplit

from requests import get
from soweego.commons import url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import discogs_entity

<< << << < HEAD
== == == =

>>>>>> > master

LOGGER = logging.getLogger(__name__)

DUMP_BASE_URL = 'https://discogs-data.s3-us-west-2.amazonaws.com/'
DUMP_LIST_URL_TEMPLATE = DUMP_BASE_URL + '?delimiter=/&prefix=data/{}/'

# From https://wikimediafoundation.org/our-work/wikimedia-projects/
WIKI_PROJECTS = [
    'wikipedia',
    'wikibooks',
    'wiktionary',
    'wikiquote',
    'commons.wikimedia',
    'wikisource',
    'wikiversity',
    'wikidata',
    'mediawiki',
    'wikivoyage',
    'meta.wikimedia'
]


class DiscogsDumpExtractor(BaseDumpExtractor):

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
        dead_links = 0
        musicians = 0
        musician_links = 0
        bands = 0
        band_links = 0
        total_entities = 0

        db_manager = DBManager()
        db_manager.drop(discogs_entity.DiscogsMusicianEntity)
        db_manager.drop(discogs_entity.DiscogsMusicianLinkEntity)
        db_manager.create(discogs_entity.DiscogsMusicianEntity)
        db_manager.create(discogs_entity.DiscogsMusicianLinkEntity)
        db_manager.drop(discogs_entity.DiscogsGroupEntity)
        db_manager.drop(discogs_entity.DiscogsGroupLinkEntity)
        db_manager.create(discogs_entity.DiscogsGroupEntity)
        db_manager.create(discogs_entity.DiscogsGroupLinkEntity)

        with gzip.open(dump_file_path, 'rt') as dump:
            for _, node in et.iterparse(dump):
                if not node.tag == 'artist':
                    continue
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

                session = db_manager.new_session()

                # Populate entities
                groups = node.find('groups')
                if groups:
                    current_entity = discogs_entity.DiscogsMusicianEntity()
                    self._fill_entity(
                        current_entity, identifier, name, node)
                    session.add(current_entity)
                    musicians += 1
                    total_entities += 1
                    self._populate_name_variations(
                        session, node, current_entity, identifier, musicians)
                    self._populate_links(
                        session, node, discogs_entity.DiscogsMusicianLinkEntity, identifier, musician_links, dead_links)
                    # TODO populate musician -> groups relationship table
                    #  for group in list(groups):
                    #      get group.attrib['id']
                members = node.find('members')
                if members:
                    current_entity = discogs_entity.DiscogsGroupEntity()
                    self._fill_entity(
                        current_entity, identifier, name, node)
                    session.add(current_entity)
                    bands += 1
                    total_entities += 1
                    self._populate_name_variations(
                        session, node, current_entity, identifier, bands)
                    self._populate_links(
                        session, node, discogs_entity.DiscogsGroupLinkEntity, identifier, band_links, dead_links)
                    # TODO populate group -> musicians relationship table
                    #  for group in list(groups):
                    #      get group.attrib['id']

                session.commit()

        end = datetime.now()
        LOGGER.info('Import completed in %d. Total entities: %d. %d musicians with %d links, %d bands with %d links, %d discarded dead links.',
                    end - start, total_entities, musicians, musician_links, bands, band_links, dead_links)

    def _populate_name_variations(self, session, artist_node, current_entity, identifier, count):
        name_variations_node = artist_node.find('namevariations')
        if name_variations_node:
            children = list(name_variations_node)
            if children:
                session.add_all(self._denormalize_name_variation_entities(
                    current_entity, children, count))
            else:
                LOGGER.debug(
                    'Artist %s has an empty <namevariations/> tag', identifier)
        else:
            LOGGER.debug(
                'Artist %s has no <namevariations> tag', identifier)

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
        # Profile
        profile = artist_node.findtext('profile')
        if profile:
            entity.profile = profile
        else:
            LOGGER.debug('Artist %s has an empty <profile/> tag', identifier)
        # Data quality
        data_quality = artist_node.findtext('data_quality')
        if data_quality:
            entity.data_quality = data_quality
        else:
            LOGGER.debug(
                'Artist %s has an empty <data_quality/> tag', identifier)

    def _denormalize_name_variation_entities(self, main_entity: discogs_entity.DiscogsBaseEntity, name_variation_nodes, count):
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
            variation_entity.profile = main_entity.profile
            variation_entity.data_quality = main_entity.data_quality
            count += 1
            yield variation_entity

    def _populate_links(self, session, artist_node, entity_class, identifier, valid_urls, dead_urls):
        urls = artist_node.find('urls')
        if urls:
            for url_element in urls.iterfind('url'):
                url = url_element.text
                if url:
                    session.add_all(self._fill_clean_link(
                        url, identifier, entity_class, valid_urls, dead_urls))
                else:
                    LOGGER.debug(
                        'Artist %s: skipping empty <url> tag', identifier)
                    continue

    def _fill_clean_link(self, url, identifier, entity_class, valid_urls, dead_urls):
        LOGGER.debug('Processing URL <%s>', url)
        clean_parts = url_utils.clean(url)
        LOGGER.debug('Clean URL: %s', clean_parts)
        for part in clean_parts:
            valid_url = url_utils.validate(part)
            if not valid_url:
                dead_urls += 1
                continue
            LOGGER.debug('Valid URL: <%s>', valid_url)
            resolved = url_utils.resolve(valid_url)
            if not resolved:
                dead_urls += 1
                continue
            LOGGER.debug('Living URL: <%s>', resolved)
            domain = urlsplit(resolved).netloc
            link_entity = entity_class()
            link_entity.catalog_id = identifier
            link_entity.url = resolved
            link_entity.is_wiki = True if any(
                wiki_project in domain for wiki_project in WIKI_PROJECTS) else False
            link_entity.tokens = '|'.join(url_utils.tokenize(resolved))
            valid_urls += 1
            yield link_entity
