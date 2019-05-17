#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generic service for dump updating/importing"""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Massimo Frasson'

import datetime
import logging
import os
from multiprocessing import Pool

import click
from tqdm import tqdm
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.commons import http_client as client, constants
from soweego.commons import target_database, url_utils
from soweego.commons.db_manager import DBManager

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.available_targets()))
@click.option('--resolve/--no-resolve', default=True,
              help='Resolves all the links to check if they are valid. Default: yes.')
@click.option('--output', '-o', default=constants.SHARED_FOLDER, type=click.Path())
def import_cli(catalog: str, resolve: bool, output: str) -> None:
    """Download, extract and import an available catalog."""

    extractor = constants.DUMP_EXTRACTOR[catalog]
    Importer().refresh_dump(output, extractor, resolve)


def _resolve_url(res):
    return url_utils.resolve(res.url), res


@click.command()
@click.argument('catalog', type=click.Choice(target_database.available_targets()))
def validate_links_cli(catalog: str):
    for entity_type in target_database.available_types_for_target(catalog):

        LOGGER.info("Validating %s %s links..." % (catalog, entity_type))
        entity = target_database.get_link_entity(catalog, entity_type)
        if not entity:
            LOGGER.info("%s %s does not have a links table. Skipping..." % (catalog, entity_type))
            continue

        session = DBManager.connect_to_db()
        total = session.query(entity).count()
        removed = 0

        with Pool() as pool:
            # Validate each link
            for resolved, res_entity in tqdm(pool.imap_unordered(_resolve_url,
                                                                 session.query(entity)), total=total):
                if not resolved:
                    session_delete = DBManager.connect_to_db()
                    # if not valid delete
                    session_delete.delete(res_entity)
                    try:
                        session_delete.commit()
                        removed += 1
                    except:
                        session.rollback()
                        raise
                    finally:
                        session_delete.close()

        session.close()
        LOGGER.info("Removed %s/%s from %s %s" % (removed, total, catalog, entity_type))


class Importer:

    def refresh_dump(self, output_folder: str, downloader: BaseDumpExtractor, resolve: bool):
        """Downloads the dump, if necessary,
        and calls the handler over the dump file"""
        filepaths = []

        for download_url in downloader.get_dump_download_urls():

            LOGGER.info("Retrieving last modified of %s" % download_url)

            last_modified = client.http_call(download_url,
                                             'HEAD').headers[const.LAST_MODIFIED]

            try:
                last_modified = datetime.datetime.strptime(
                    last_modified, '%a, %d %b %Y %H:%M:%S GMT').strftime('%Y%m%d_%H%M%S')
            except TypeError:
                LOGGER.info(
                    "Last modified not available, using now as replacement")
                last_modified = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

            extensions = download_url.split('/')[-1].split('.')[1:]

            file_name = "%s.%s" % (last_modified, '.'.join(extensions))

            file_full_path = os.path.join(output_folder, file_name)

            # Check if the current dump is up-to-date
            if not os.path.isfile(file_full_path):
                LOGGER.info(
                    "%s not previously downloaded, downloading now..." % download_url)
                self._update_dump(download_url, file_full_path)
            filepaths.append(file_full_path)

        downloader.extract_and_populate(filepaths, resolve)

    def _update_dump(self, dump_url: str, file_output_path: str):
        """Download the dump"""
        client.download_file(dump_url, file_output_path)
