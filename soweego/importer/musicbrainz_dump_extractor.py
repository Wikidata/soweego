#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MusicBrainz dump extractor"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

import json
import logging
import os
import tarfile
from collections import defaultdict
from csv import DictReader
from datetime import date

import requests
from soweego.commons import url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.musicbrainz_entity import (MusicbrainzBandEntity,
                                                        MusicBrainzLink,
                                                        MusicbrainzPersonEntity)

LOGGER = logging.getLogger(__name__)


class MusicBrainzDumpExtractor(BaseDumpExtractor):

    def get_dump_download_url(self) -> str:
        latest_version = requests.get(
            'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST').text.rstrip()
        return 'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/%s/mbdump.tar.bz2' % latest_version

    def extract_and_populate(self, dump_file_path):
        dump_path = os.path.join(os.path.dirname(
            os.path.abspath(dump_file_path)), "%s_%s" % (os.path.basename(dump_file_path), 'extracted'))

        if not os.path.isdir(dump_path):
            with tarfile.open(dump_file_path, "r:bz2") as tar:
                tar.extractall(dump_path)

        db_manager = DBManager()
        db_manager.drop(MusicbrainzPersonEntity)
        db_manager.create(MusicbrainzPersonEntity)
        db_manager.drop(MusicbrainzBandEntity)
        db_manager.create(MusicbrainzBandEntity)
        db_manager.drop(MusicBrainzLink)
        db_manager.create(MusicBrainzLink)

        artist_count = 0
        for artist in self._artist_generator(dump_path):
            artist_count = artist_count + 1
            session = db_manager.new_session()
            session.add(artist)
            session.commit()

        print("Added %s artist records" % artist_count)

        link_count = 0
        for link in self._link_generator(dump_path):
            link_count = link_count + 1
            session = db_manager.new_session()
            session.add(link)
            session.commit()

        print("Added %s link records" % link_count)

    def _link_generator(self, dump_path):
        l_artist_url_path = os.path.join(dump_path, 'mbdump', 'l_artist_url')

        urlid_artistid_relationship = {}

        with open(l_artist_url_path, "r") as tsvfile:
            url_relationships = DictReader(tsvfile,
                                           delimiter='\t',
                                           fieldnames=[i for i in range(0, 6)])
            for relationship in url_relationships:
                # url id matched with its user id
                if relationship[3] in urlid_artistid_relationship:
                    LOGGER.warning(
                        'Url with ID %s has multiple artists, only one will be stored' % relationship[3])
                else:
                    urlid_artistid_relationship[relationship[3]
                                                ] = relationship[2]

        url_artistid = {}
        url_path = os.path.join(dump_path, 'mbdump', 'url')

        with open(url_path, "r") as tsvfile:
            urls = DictReader(tsvfile,
                              delimiter='\t',
                              fieldnames=[i for i in range(0, 5)])
            for url_record in urls:
                urlid = url_record[0]
                if urlid in urlid_artistid_relationship:
                    url_artistid[url_record[2]
                                 ] = urlid_artistid_relationship[urlid]
                    del urlid_artistid_relationship[urlid]

        urlid_artistid_relationship = None

        artistid_url = defaultdict(list)

        for url, artistid in url_artistid.items():
            artistid_url[artistid].append(url)

        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(artistfile, delimiter='\t', fieldnames=['id', 'gid', 'label', 'sort_label', 'b_year', 'b_month', 'b_day', 'd_year', 'd_month', 'd_day', 'type_id']):
                if artist['id'] in artistid_url:
                    for link in artistid_url[artist['id']]:
                        for candidate_url in url_utils.clean(link):
                            if not url_utils.validate(candidate_url):
                                continue
                            if not url_utils.resolve(candidate_url):
                                continue
                            current_entity = MusicBrainzLink()
                            current_entity.catalog_id = artist['gid']
                            current_entity.url = candidate_url
                            current_entity.is_wiki = url_utils.is_wiki_link(
                                candidate_url)
                            current_entity.tokens = ' '.join(
                                url_utils.tokenize(candidate_url))
                            yield current_entity

    def _artist_generator(self, dump_path):
        artist_alias_path = os.path.join(dump_path, 'mbdump', 'artist_alias')
        artist_path = os.path.join(dump_path, 'mbdump', 'artist')

        aliases = defaultdict(list)

        # Key is the entity id which has a list of aliases
        with open(artist_alias_path, 'r') as aliasesfile:
            for alias in DictReader(aliasesfile, delimiter='\t', fieldnames=[
                    'id', 'parent_id', 'label']):
                aliases[alias['parent_id']].append(alias['label'])

        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(artistfile, delimiter='\t', fieldnames=['id', 'gid', 'label', 'sort_label', 'b_year', 'b_month', 'b_day', 'd_year', 'd_month', 'd_day', 'type_id']):
                if self._check_person(artist['type_id']):
                    current_entity = MusicbrainzPersonEntity()

                    try:
                        self._fill_entity(current_entity, artist)
                    except ValueError:
                        LOGGER.error('Wrong date: %s', artist)
                        continue

                    yield current_entity

                    # Creates an entity foreach available alias
                    for alias in self._alias_entities(
                            current_entity, MusicbrainzPersonEntity, aliases[artist['id']]):
                        yield alias

                if self._check_band(artist['type_id']):
                    current_entity = MusicbrainzBandEntity()

                    try:
                        self._fill_entity(current_entity, artist)
                    except ValueError:
                        LOGGER.error('Wrong date: %s', artist)
                        continue

                    yield current_entity

                    # Creates an entity foreach available alias
                    for alias in self._alias_entities(
                            current_entity, MusicbrainzPersonEntity, aliases[artist['id']]):
                        yield alias

    def _fill_entity(self, entity: BaseEntity, info):
        entity.catalog_id = info['gid']
        entity.name = info['label']
        birth_date = self._get_date_and_precision(
            info['b_year'], info['b_month'], info['b_day'])
        death_date = self._get_date_and_precision(
            info['d_year'], info['d_month'], info['d_day'])
        entity.born = birth_date[0]
        entity.born_precision = birth_date[1]
        entity.died = death_date[0]
        entity.died_precision = death_date[1]

    def _alias_entities(self, entity: BaseEntity, aliases_class, aliases: []):
        for alias_label in aliases:
            alias_entity = aliases_class()
            alias_entity.catalog_id = entity.catalog_id
            alias_entity.born = entity.born
            alias_entity.born_precision = entity.born_precision
            alias_entity.died = entity.died
            alias_entity.died_precision = entity.died_precision

            alias_entity.name = alias_label
            yield alias_entity

    def _get_date_and_precision(self, year, month, day):
        date_list = [year, month, day]
        precision = -1
        try:
            null_index = date_list.index('\\N')
            precision = 8 + null_index if null_index > 0 else -1
        except ValueError:
            precision = 11

        date_list = ['0001' if i == '\\N' else i for i in date_list]

        if precision == -1:
            return (None, None)

        return (date(int(date_list[0]), int(date_list[1]), int(date_list[2])), precision)

    def _check_person(self, type_code):
        return type_code in ['1', '4', '3', '\\N']

    def _check_band(self, type_code):
        return type_code in ['2', '5', '6', '3', '\\N']
