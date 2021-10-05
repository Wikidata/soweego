#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Download, extract, and import a supported catalog."""

__author__ = 'Massimo Frasson, Marco Fossati'
__email__ = 'maxfrax@gmail.com, fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, MaxFrax96, Hjfocs'

import csv
import datetime
import logging
import os
from multiprocessing import Pool

import click
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from soweego.commons import constants
from soweego.commons import http_client as client
from soweego.commons import keys, target_database, url_utils
from soweego.commons.db_manager import DBManager
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.discogs_dump_extractor import DiscogsDumpExtractor
from soweego.importer.imdb_dump_extractor import IMDbDumpExtractor
from soweego.importer.musicbrainz_dump_extractor import MusicBrainzDumpExtractor

LOGGER = logging.getLogger(__name__)

DUMP_EXTRACTOR = {
    keys.DISCOGS: DiscogsDumpExtractor,
    keys.IMDB: IMDbDumpExtractor,
    keys.MUSICBRAINZ: MusicBrainzDumpExtractor,
}
ROTTEN_URLS_FNAME = '{catalog}_{entity}_rotten_urls.csv'


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.option(
    '--url-check',
    is_flag=True,
    help=(
        'Check for rotten URLs while importing. Default: no. '
        'WARNING: this will dramatically increase the import time.'
    ),
)
@click.option(
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.WORK_DIR,
    help=f'Input/output directory, default: {constants.WORK_DIR}.',
)
def import_cli(catalog: str, url_check: bool, dir_io: str) -> None:
    """Download, extract, and import a supported catalog."""

    extractor = DUMP_EXTRACTOR[catalog]()

    Importer().refresh_dump(dir_io, extractor, url_check)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.option(
    '-d',
    '--drop',
    is_flag=True,
    help=f'Drop rotten URLs from the DB.',
)
@click.option(
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.WORK_DIR,
    help=f'Input/output directory, default: {constants.WORK_DIR}.',
)
def check_urls_cli(catalog, drop, dir_io):
    """Check for rotten URLs of an imported catalog.

    For every catalog entity, dump rotten URLs to a file.
    CSV format: URL,catalog_ID

    Use '-d' to drop rotten URLs from the DB on the fly.
    """
    for entity in target_database.supported_entities_for_target(catalog):
        out_path = os.path.join(
            dir_io, ROTTEN_URLS_FNAME.format(catalog=catalog, entity=entity)
        )

        LOGGER.info('Starting check of %s %s URLs ...', catalog, entity)
        link_entity = target_database.get_link_entity(catalog, entity)
        if not link_entity:
            LOGGER.info(
                '%s %s does not have a links table. Skipping ...',
                catalog,
                entity,
            )
            continue

        query_session = DBManager.connect_to_db()
        total = query_session.query(link_entity).count()

        rotten = 0
        if drop:
            removed = 0

        # Parallel operation
        with Pool() as pool, open(out_path, 'w', buffering=1) as fout:
            writer = csv.writer(fout)
            try:
                # Resolve every URL
                for resolved, result in tqdm(
                    pool.imap_unordered(_resolve, query_session.query(link_entity)),
                    total=total,
                ):
                    if not resolved:
                        # Dump
                        writer.writerow((result.url, result.catalog_id))
                        rotten += 1

                        # Drop from DB
                        if drop:
                            delete_session = DBManager.connect_to_db()
                            delete_session.delete(result)
                            try:
                                delete_session.commit()
                                removed += 1
                            except SQLAlchemyError as error:
                                LOGGER.error(
                                    'Failed deletion of %s: %s',
                                    result,
                                    error.__class__.__name__,
                                )
                                LOGGER.debug(error)
                                delete_session.rollback()
                            finally:
                                delete_session.close()
            except SQLAlchemyError as error:
                LOGGER.error(
                    '%s while querying %s %s URLs',
                    error.__class__.__name__,
                    catalog,
                    entity,
                )
                LOGGER.debug(error)
                session.rollback()
            finally:
                query_session.close()

        LOGGER.debug('Cache information: %s', url_utils.resolve.cache_info())
        LOGGER.info(
            "Total %s %s rotten URLs dumped to '%s': %d / %d",
            catalog,
            entity,
            out_path,
            rotten,
            total,
        )

        if drop:
            LOGGER.info(
                'Total %s %s rotten URLs dropped from the DB: %d / %d',
                catalog,
                entity,
                rotten,
                removed,
            )


def _resolve(link_entity):
    return url_utils.resolve(link_entity.url), link_entity


class Importer:
    """Handle a catalog dump: check its freshness and dispatch the appropriate
    extractor."""

    def refresh_dump(
        self, output_folder: str, extractor: BaseDumpExtractor, resolve: bool
    ):
        """Eventually download the latest dump, and call the
         corresponding extractor.

        :param output_folder: a path where the downloaded dumps will be stored
        :param extractor: :class:`~soweego.importer.base_dump_extractor.BaseDumpExtractor`
          implementation to process the dump
        :param resolve: whether to resolve URLs found in catalog dumps or not
        """
        filepaths = []

        for download_url in extractor.get_dump_download_urls():

            LOGGER.info("Retrieving last modified of %s", download_url)

            last_modified = client.http_call(download_url, 'HEAD').headers[
                keys.LAST_MODIFIED
            ]

            try:
                last_modified = datetime.datetime.strptime(
                    last_modified, '%a, %d %b %Y %H:%M:%S GMT'
                ).strftime('%Y%m%d_%H%M%S')
            except TypeError:
                LOGGER.info("Last modified not available, using now as replacement")
                last_modified = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

            extensions = download_url.split('/')[-1].split('.')[1:]

            file_name = "%s.%s" % (last_modified, '.'.join(extensions))

            file_full_path = os.path.join(output_folder, file_name)

            # Check if the current dump is up-to-date
            if not os.path.isfile(file_full_path):
                LOGGER.info(
                    "%s not previously downloaded, downloading now...",
                    download_url,
                )
                self._update_dump(download_url, file_full_path)
            filepaths.append(file_full_path)

        extractor.extract_and_populate(filepaths, resolve)

    @staticmethod
    def _update_dump(dump_url: str, file_output_path: str):
        """Download the dump."""
        client.download_file(dump_url, file_output_path)
