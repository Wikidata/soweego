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
import re
import tarfile
from collections import defaultdict
from csv import DictReader
from datetime import date
from typing import Iterable

import requests
from sqlalchemy.exc import IntegrityError

from soweego.commons import text_utils, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.musicbrainz_entity import (ARTIST_TABLE,
                                                        MusicBrainzArtistBandRelationship,
                                                        MusicbrainzArtistEntity,
                                                        MusicbrainzArtistLinkEntity,
                                                        MusicbrainzBandEntity,
                                                        MusicbrainzBandLinkEntity)
from soweego.wikidata.sparql_queries import external_id_pids_and_urls_query

LOGGER = logging.getLogger(__name__)


class MusicBrainzDumpExtractor(BaseDumpExtractor):

    def get_dump_download_urls(self) -> Iterable[str]:
        latest_version = requests.get(
            'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST').text.rstrip()
        return ['http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/%s/mbdump.tar.bz2' % latest_version]

    def extract_and_populate(self, dump_file_paths: Iterable[str]):
        dump_file_path = dump_file_paths[0]
        dump_path = os.path.join(os.path.dirname(
            os.path.abspath(dump_file_path)), "%s_%s" % (os.path.basename(dump_file_path), 'extracted'))

        if not os.path.isdir(dump_path):
            with tarfile.open(dump_file_path, "r:bz2") as tar:
                LOGGER.info("Extracting dump %s in %s" %
                            (dump_file_path, dump_path))
                tar.extractall(dump_path)
                LOGGER.info("Extracted dump %s in %s" %
                            (dump_file_path, dump_path))

        tables = [MusicbrainzArtistEntity,
                  MusicbrainzBandEntity]

        db_manager = DBManager()
        db_manager.drop(tables)
        db_manager.create(tables)

        LOGGER.info("Dropped and created tables %s" % tables)
        LOGGER.info("Importing artists and bands")

        artist_count = 0
        for artist in self._artist_generator(dump_path):
            artist_count = artist_count + 1
            self._commit_entity(db_manager, artist)

        LOGGER.debug("Added %s artist records" % artist_count)

        db_manager.drop([MusicbrainzArtistLinkEntity,
                         MusicbrainzBandLinkEntity])
        db_manager.create([MusicbrainzArtistLinkEntity,
                           MusicbrainzBandLinkEntity])

        LOGGER.info("Dropped and created tables %s" % tables)
        LOGGER.info("Importing links")

        link_count = 0
        for link in self._link_generator(dump_path):
            link_count = link_count + 1
            self._commit_entity(db_manager, link)

        LOGGER.debug("Added %s link records" % link_count)
        LOGGER.info("Importing ISNIs")

        isni_link_count = 0
        for link in self._isni_link_generator(dump_path):
            isni_link_count = isni_link_count + 1
            self._commit_entity(db_manager, link)

        LOGGER.debug("Added %s ISNI link records" % isni_link_count)

        db_manager.drop([MusicBrainzArtistBandRelationship])
        db_manager.create([MusicBrainzArtistBandRelationship])
        LOGGER.info("Dropped and created tables %s" % tables)
        LOGGER.info("Importing relationships artist-band")

        relationships_count = 0
        relationships_total = 0
        for relationship in self._artist_band_relationship_generator(dump_path):
            try:
                relationships_total = relationships_total + 1
                self._commit_entity(db_manager, relationship)
                relationships_count = relationships_count + 1
            except IntegrityError as i:
                LOGGER.warning(str(i))

        LOGGER.debug("Added %s/%s relationships records" %
                     (relationships_count, relationships_total))

    def _link_generator(self, dump_path):
        l_artist_url_path = os.path.join(dump_path, 'mbdump', 'l_artist_url')

        # Loads all the relationships between URL ID and ARTIST ID
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
        # Translates URL IDs to the relative URL
        with open(url_path, "r") as tsvfile:
            urls = DictReader(tsvfile,
                              delimiter='\t',
                              fieldnames=[i for i in range(0, 5)])
            for url_record in urls:
                urlid = url_record[0]
                if urlid in urlid_artistid_relationship:
                    for candidate_url in url_utils.clean(url_record[2]):
                        if not url_utils.validate(candidate_url):
                            continue
                        if not url_utils.resolve(candidate_url):
                            continue
                        url_artistid[candidate_url] = urlid_artistid_relationship[urlid]
                        del urlid_artistid_relationship[urlid]

        urlid_artistid_relationship = None

        artistid_url = defaultdict(list)
        # Inverts dictionary
        for url, artistid in url_artistid.items():
            artistid_url[artistid].append(url)

        url_artistid = None
        # Translates ARTIST ID to the relative ARTIST
        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(artistfile, delimiter='\t',
                                     fieldnames=['id', 'gid', 'label', 'sort_label', 'b_year', 'b_month', 'b_day',
                                                 'd_year', 'd_month', 'd_day', 'type_id']):
                if artist['id'] in artistid_url:
                    for link in artistid_url[artist['id']]:
                        if self._check_person(artist['type_id']):
                            current_entity = MusicbrainzArtistLinkEntity()
                            self._fill_link_entity(
                                current_entity, artist['gid'], link)
                            yield current_entity
                        if self._check_band(artist['type_id']):
                            current_entity = MusicbrainzBandLinkEntity()
                            self._fill_link_entity(
                                current_entity, artist['gid'], link)
                            yield current_entity

    def _isni_link_generator(self, dump_path):
        isni_file_path = os.path.join(dump_path, 'mbdump', 'artist_isni')

        artist_link = {}

        done = False
        for result in external_id_pids_and_urls_query():
            if done:
                break
            for pid, formatter in result.items():
                if pid == 'P213':
                    for url_formatter, regex in formatter.items():
                        r = re.compile(regex)

                        with open(isni_file_path, 'r') as artistfile:
                            for artistid_isni in DictReader(artistfile, delimiter='\t', fieldnames=['id', 'isni']):
                                # If ISNI is valid, generates an url for the artist
                                artistid = artistid_isni['id']
                                isni = artistid_isni['isni']

                                link = url_formatter.replace(
                                    '$1', isni)
                                for candidate_url in url_utils.clean(link):
                                    if not url_utils.validate(candidate_url):
                                        continue
                                    if not url_utils.resolve(candidate_url):
                                        continue
                                    artist_link[artistid] = candidate_url
                    done = True

        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(artistfile, delimiter='\t',
                                     fieldnames=['id', 'gid', 'label', 'sort_label', 'b_year', 'b_month', 'b_day',
                                                 'd_year', 'd_month', 'd_day', 'type_id']):
                try:
                    # Checks if artist has isni
                    link = artist_link[artist['id']]
                    if self._check_person(artist['type_id']):
                        current_entity = MusicbrainzArtistLinkEntity()
                        self._fill_link_entity(
                            current_entity, artist['gid'], link)
                        yield current_entity
                    if self._check_band(artist['type_id']):
                        current_entity = MusicbrainzBandLinkEntity()
                        self._fill_link_entity(
                            current_entity, artist['gid'], link)
                        yield current_entity
                except KeyError:
                    continue

    def _artist_generator(self, dump_path):
        artist_alias_path = os.path.join(dump_path, 'mbdump', 'artist_alias')
        artist_path = os.path.join(dump_path, 'mbdump', 'artist')
        area_path = os.path.join(dump_path, 'mbdump', 'area')

        aliases = defaultdict(list)
        areas = {}

        # Key is the entity id which has a list of aliases
        with open(artist_alias_path, 'r') as aliasesfile:
            for alias in DictReader(aliasesfile, delimiter='\t', fieldnames=[
                'id', 'parent_id', 'label']):
                aliases[alias['parent_id']].append(alias['label'])

        # Key is the area internal id, value is the name
        with open(area_path, 'r') as areafile:
            for area in DictReader(areafile, delimiter='\t', fieldnames=['id', 'gid', 'name']):
                areas[area['id']] = area['name'].lower()

        with open(artist_path, 'r') as artistfile:
            for artist in DictReader(artistfile, delimiter='\t',
                                     fieldnames=['id', 'gid', 'label', 'sort_label', 'b_year', 'b_month', 'b_day',
                                                 'd_year', 'd_month', 'd_day', 'type_id', 'area', 'gender', 'ND1',
                                                 'ND2', 'ND3', 'ND4', 'b_place', 'd_place']):
                if self._check_person(artist['type_id']):
                    current_entity = MusicbrainzArtistEntity()

                    try:
                        self._fill_entity(current_entity, artist, areas)
                        current_entity.gender = self._artist_gender(
                            artist['gender'])
                    except ValueError:
                        LOGGER.error('Wrong date: %s', artist)
                        continue

                    # Creates an entity foreach available alias
                    for alias in self._alias_entities(
                            current_entity, MusicbrainzArtistEntity, aliases[artist['id']]):
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
                            current_entity, MusicbrainzBandEntity, aliases[artist['id']]):
                        yield alias

                    yield current_entity

    def _artist_band_relationship_generator(self, dump_path):
        link_types = set(['855', '103', '305', '965', '895'])
        link_file_path = os.path.join(dump_path, 'mbdump', 'link')
        to_invert = set()

        links = set()
        with open(link_file_path) as link_file:
            reader = DictReader(link_file,
                                delimiter='\t',
                                fieldnames=['id', 'link_type'])
            for row in reader:
                if row['link_type'] in link_types:
                    links.add(row['id'])

        artists_relationship_file = os.path.join(
            dump_path, 'mbdump', 'l_artist_artist')

        ids_translator = {}
        relationships = []
        with open(artists_relationship_file) as relfile:
            reader = DictReader(relfile, delimiter='\t', fieldnames=[
                'id', 'link_id', 'entity0', 'entity1'])
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
            for artist in DictReader(artistfile, delimiter='\t', fieldnames=['id', 'gid']):
                if artist['id'] in ids_translator:
                    ids_translator[artist['id']] = artist['gid']

        for relation in relationships:
            translation0, translation1 = ids_translator[relation[0]
                                         ], ids_translator[relation[1]]

            if translation0 and translation1:
                if relation in to_invert:
                    yield MusicBrainzArtistBandRelationship(translation1, translation0)
                else:
                    yield MusicBrainzArtistBandRelationship(translation0, translation1)
            else:
                LOGGER.warning("Artist id missing translation: %s to (%s, %s)" %
                               (relation, translation0, translation1))

    def _fill_entity(self, entity, info, areas):
        entity.catalog_id = info['gid']
        entity.name = info['label']
        name_tokens = text_utils.tokenize(info['label'])
        if name_tokens:
            entity.name_tokens = ' '.join(name_tokens)
        birth_date = self._get_date_and_precision(
            info['b_year'], info['b_month'], info['b_day'])
        death_date = self._get_date_and_precision(
            info['d_year'], info['d_month'], info['d_day'])
        entity.born = birth_date[0]
        entity.born_precision = birth_date[1]
        entity.died = death_date[0]
        entity.died_precision = death_date[1]
        try:
            entity.birth_place = areas[info['b_place']]
        except KeyError:
            entity.birth_place = None
        try:
            entity.death_place = areas[info['d_place']]
        except KeyError:
            entity.death_place = None

    def _fill_link_entity(self, entity, gid, link):
        entity.catalog_id = gid
        entity.url = link
        entity.is_wiki = url_utils.is_wiki_link(link)
        url_tokens = url_utils.tokenize(link)
        if url_tokens:
            entity.url_tokens = ' '.join(url_tokens)

    def _alias_entities(self, entity: BaseEntity, aliases_class, aliases: []):
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

    def _artist_gender(self, gender_code):
        genders = {'1': 'male', '2': 'female', '3': 'other', '\\N': 'other'}
        return genders[gender_code]
