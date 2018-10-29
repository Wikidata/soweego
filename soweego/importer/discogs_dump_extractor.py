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
from datetime import date
from urllib.parse import urlsplit

from requests import get

from soweego.commons.db_manager import DBManager
from soweego.commons.file_utils import get_path
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models import discogs_entity

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
        db_manager = DBManager(
            get_path('soweego.importer.resources', 'db_credentials.json'))
        db_manager.drop(discogs_entity.DiscogsMusicianEntity)
        db_manager.create(discogs_entity.DiscogsMusicianEntity)
        db_manager.drop(discogs_entity.DiscogsGroupEntity)
        db_manager.create(discogs_entity.DiscogsGroupEntity)

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
                current_entity = None
                groups = node.find('groups')
                if groups:
                    current_entity = discogs_entity.DiscogsMusicianEntity()
                    self._fill_entity(
                        current_entity, identifier, name, node)
                    self._populate_name_variations(
                        node, session, current_entity, identifier)
                    # TODO populate musician -> groups relationship table
                    #  for group in list(groups):
                    #      get group.attrib['id']

                members = node.find('members')
                if members:
                    current_entity = discogs_entity.DiscogsGroupEntity()
                    self._fill_entity(
                        current_entity, identifier, name, node)
                    self._populate_name_variations(
                        node, session, current_entity, identifier)
                    # TODO populate group -> musicians relationship table
                    #  for group in list(groups):
                    #      get group.attrib['id']

                if current_entity:
                    session.add(current_entity)
                session.commit()

    def _populate_name_variations(self, node, session, current_entity, identifier):
        name_variations_node = node.find('namevariations')
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
            variation_entity.profile = main_entity.profile
            variation_entity.data_quality = main_entity.data_quality
            yield variation_entity
