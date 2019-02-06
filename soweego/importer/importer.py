#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generic service for dump updating/importing"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import datetime
import logging
import os

import click
from soweego.commons import constants as const
from soweego.commons import http_client as client
from soweego.importer.base_dump_extractor import BaseDumpExtractor
from soweego.importer.discogs_dump_extractor import DiscogsDumpExtractor
from soweego.importer.musicbrainz_dump_extractor import \
    MusicBrainzDumpExtractor

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(['discogs', 'musicbrainz']))
@click.option('--output', '-o', default='/app/shared', type=click.Path())
def import_cli(catalog: str, output: str) -> None:
    """Download, extract and import an available catalog."""
    importer = Importer()
    extractor = BaseDumpExtractor()

    if catalog == 'discogs':
        extractor = DiscogsDumpExtractor()
    elif catalog == 'musicbrainz':
        extractor = MusicBrainzDumpExtractor()

    importer.refresh_dump(output, extractor)


class Importer():

    def refresh_dump(self, output_folder: str, downloader: BaseDumpExtractor):
        """Downloads the dump, if necessary,
        and calls the handler over the dump file"""
        filepaths = []

        for download_url in downloader.get_dump_download_urls():

            LOGGER.info("Retrieving last modified of %s" % download_url)

            last_modified = client.http_call(download_url,
                                             'HEAD').headers[const.LAST_MODIFIED_KEY]

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

        downloader.extract_and_populate(filepaths)

    def _update_dump(self, dump_url: str, file_output_path: str) -> None:
        """Download the dump"""
        client.download_file(dump_url, file_output_path)
