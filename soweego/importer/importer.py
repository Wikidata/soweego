#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Download, extract, and import a supported catalog."""

__author__ = 'Massimo Frasson'
__email__ = 'maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, MaxFrax96'

import datetime
import logging
import os
from multiprocessing import Pool

import click
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


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.option(
    '--url-check',
    is_flag=True,
    help=(
        'Check for rotten URLs while importing. Default: no. '
        'WARNING: this will dramatically increase the import time.'
    ),
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def import_cli(catalog: str, url_check: bool, dir_io: str) -> None:
    """Download, extract, and import a supported catalog."""

    extractor = DUMP_EXTRACTOR[catalog]()

    Importer().refresh_dump(dir_io, extractor, url_check)


def _resolve_url(res):
    return url_utils.resolve(res.url), res


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
def check_links_cli(catalog: str):
    """Check for rotten URLs of an imported catalog."""
    for entity_type in target_database.supported_entities_for_target(catalog):

        LOGGER.info("Validating %s %s links...", catalog, entity_type)
        entity = target_database.get_link_entity(catalog, entity_type)
        if not entity:
            LOGGER.info(
                "%s %s does not have a links table. Skipping...",
                catalog,
                entity_type,
            )
            continue

        session = DBManager.connect_to_db()
        total = session.query(entity).count()
        removed = 0

        with Pool() as pool:
            # Validate each link
            for resolved, res_entity in tqdm(
                pool.imap_unordered(_resolve_url, session.query(entity)),
                total=total,
            ):
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
        LOGGER.info(
            "Removed %s/%s from %s %s", removed, total, catalog, entity_type
        )


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
                LOGGER.info(
                    "Last modified not available, using now as replacement"
                )
                last_modified = datetime.datetime.now().strftime(
                    '%Y%m%d_%H%M%S'
                )

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
