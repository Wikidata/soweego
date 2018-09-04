#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import logging
import xml.etree.ElementTree as et

from soweego.importer.models.orm.discogs_musician_entity import \
    DiscogsMusicianEntity
from soweego.importer.services.db_manager import DBManager

LOGGER = logging.getLogger(__name__)


def handle(file_path: str, mappings: dict, orm_model: DiscogsMusicianEntity):
    db_manager = DBManager('db_credential_path')
    session = db_manager.new_session()
    # TODO improve extract_data_from_dump
    # TODO this is one <artist> node
    current_entity = orm_model()
    # TODO populate current_entity attributes accordingly
    session.add(current_entity)
    # e.g., current_entity.catalog_id = element.findtext('id')
    # At the end
    session.commit()


def extract_data_from_dump(dump_file_path):
    """Extract the set of identifiers and 3 dictionaries ``{name|link|wikilink: identifier}``
    from a Discogs dump file path.

    Dumps available at https://data.discogs.com/
    """
    ids = set()
    names = {}
    links = {}
    wikilinks = {}
    with gzip.open(dump_file_path, 'rt') as dump:
        for event, element in et.iterparse(dump):
            if element.tag == 'artist':
                identifier = element.findtext('id')
                ids.add(identifier)
                # Names
                name = element.findtext('name')
                if name:
                    names[name] = identifier
                else:
                    LOGGER.warning(
                        'Skipping extraction for identifier with no name: %s', identifier)
                    continue
                real_name = element.findtext('realname')
                if real_name:
                    names[real_name] = identifier
                else:
                    LOGGER.debug(
                        'Artist %s has an empty <realname> tag', identifier)
                variations = element.find('namevariations')
                if variations:
                    for variation_element in variations.iterfind('name'):
                        variation = variation_element.text
                        if variation:
                            names[variation] = identifier
                        else:
                            LOGGER.debug(
                                'Artist %s has an empty <namevariations> tag', identifier)
                # Links & Wiki links
                urls = element.find('urls')
                if urls:
                    for url_element in urls.iterfind('url'):
                        url = url_element.text
                        if url:
                            try:
                                domain = urlsplit(url).netloc
                                if any(wiki_project in domain for wiki_project in WIKI_PROJECTS):
                                    wikilinks[url] = identifier
                                else:
                                    links[url] = identifier
                            except ValueError as value_error:
                                LOGGER.warning(
                                    "Skipping %s: '%s'", value_error, url)
                                continue
                        else:
                            LOGGER.debug(
                                'Artist %s: skipping empty <url> tag', identifier)
                            continue
    return ids, names, links, wikilinks
