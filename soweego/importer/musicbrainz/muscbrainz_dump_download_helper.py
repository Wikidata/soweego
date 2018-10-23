
#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

import logging
import os
import tarfile
from collections import defaultdict
from csv import DictReader
from datetime import date

import requests
import sqlalchemy
from soweego.commons.db_manager import DBManager
from soweego.commons.file_utils import get_path
from soweego.commons.models.base_entity import BaseEntity
from soweego.commons.models.musicbrainz_entity import MusicbrainzPersonEntity
from soweego.importer.commons.models.base_dump_download_helper import \
    BaseDumpDownloadHelper

LOGGER = logging.getLogger(__name__)


class MusicbrainzDumpDownloadHelper(BaseDumpDownloadHelper):

    def dump_download_uri(self) -> str:
        latest_version = requests.get(
            'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST').text.rstrip()
        return 'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/%s/mbdump.tar.bz2' % latest_version

    def import_from_dump(self, tar_dump_path):
        # TODO improve dump folder name
        dump_path = os.path.join(os.path.dirname(
            os.path.abspath(tar_dump_path)), 'dump')
        with tarfile.open(tar_dump_path, "r:bz2") as tar:
            tar.extractall(dump_path)

        # TODO from pkgutil import get_data
        db_manager = DBManager(
            get_path('soweego.importer.resources', 'db_credentials.json'))
        db_manager.drop(MusicbrainzPersonEntity)
        db_manager.create(MusicbrainzPersonEntity)
        db_manager.drop(MusicbrainzBandEntity)
        db_manager.create(MusicbrainzBandEntity)

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
                session = db_manager.new_session()
                if self._check_person(artist['type_id']):
                    current_entity = MusicbrainzPersonEntity()

                    try:
                        self._fill_entity(current_entity, artist)
                    except ValueError:
                        LOGGER.error('Wrong date: %s' % artist)
                        continue

                    session.add(current_entity)

                    # Creates an entity foreach available alias
                    session.add_all(self._alias_entities(
                        current_entity, MusicbrainzPersonEntity, aliases[artist['id']]))

                if self._check_band(artist['type_id']):
                    current_entity = MusicbrainzBandEntity()

                    try:
                        self._fill_entity(current_entity, artist)
                    except ValueError:
                        LOGGER.error('Wrong date: %s' % artist)
                        continue

                    session.add(current_entity)

                    # Creates an entity foreach available alias
                    session.add_all(self._alias_entities(
                        current_entity, MusicbrainzPersonEntity, aliases[artist['id']]))

                session.commit()

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
