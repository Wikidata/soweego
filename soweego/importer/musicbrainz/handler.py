
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
from soweego.commons.models.musicbrainz_entity import MusicbrainzEntity
from soweego.importer.commons.models.dump_state import DumpState

LOGGER = logging.getLogger(__name__)


def handler(tar_dump_path):
    dump_path = os.path.join(os.path.dirname(
        os.path.abspath(tar_dump_path)), 'dump')
    with tarfile.open(tar_dump_path, "r:bz2") as tar:
        tar.extractall(dump_path)

    # TODO is this get_path the right way to do it?
    db_manager = DBManager(
        get_path('soweego.importer.resources', 'db_credentials.json'))
    db_manager.drop(MusicbrainzEntity)
    db_manager.create(MusicbrainzEntity)

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
            if _check_person(artist['type_id']):
                current_entity = MusicbrainzEntity()
                current_entity.catalog_id = artist['gid']
                current_entity.name = artist['label']

                try:
                    birth_date = _get_date_and_precision(
                        artist['b_year'], artist['b_month'], artist['b_day'])
                    death_date = _get_date_and_precision(
                        artist['d_year'], artist['d_month'], artist['d_day'])
                    current_entity.born = birth_date[0]
                    current_entity.born_precision = birth_date[1]
                    current_entity.died = death_date[0]
                    current_entity.died_precision = death_date[1]
                except ValueError:
                    LOGGER.error('Wrong date: %s' % artist)
                    continue

                session.add(current_entity)

                # Creates an entity foreach available alias
                for alias_label in aliases[current_entity.catalog_id]:
                    alias_entity = MusicbrainzEntity()
                    alias_entity.catalog_id = current_entity.catalog_id
                    alias_entity.born = current_entity.born
                    alias_entity.born_precision = current_entity.born_precision
                    alias_entity.died = current_entity.died
                    alias_entity.died_precision = current_entity.died_precision

                    alias_entity.name = alias_label
                    session.add(alias_entity)

            session.commit()

# TODO handle links


def dump_state(output, last_modified):
    latest_version = requests.get(
        'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST').text.rstrip()
    download_url = 'http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/%s/mbdump.tar.bz2' % latest_version
    return DumpState(output, download_url, last_modified)


def _get_date_and_precision(year, month, day):
    date_list = [year, month, day]
    precision = -1
    try:
        null_index = date_list.index('\\N')
        precision = 8 + null_index if null_index > 0 else -1
    except ValueError:
        precision = 11

    date_list = ['0001' if i == '\\N' else i for i in date_list]
    return (date(int(date_list[0]), int(date_list[1]), int(date_list[2])), precision)


def _check_person(type_code):
    return type_code == '1' or type_code == '\\N'
