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
from soweego.commons import localizations as loc
from soweego.importer.base_dump_downloader import BaseDumpDownloader
from soweego.importer.discogs_dump_downloader import DiscogsDumpDownloader
from soweego.importer.musicbrainz_dump_downloader import \
    MusicBrainzDumpDownloader

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(['discogs', 'musicbrainz']))
@click.option('--download-url', '-du', default=None)
@click.option('--output', '-o', default='output', type=click.Path())
def import_cli(catalog: str, download_url: str, output: str) -> None:
    """Check if there is an updated dump in the output path;
       if not, download the dump"""
    importer = Importer()
    downloader = BaseDumpDownloader()

    if catalog == 'discogs':
        downloader = DiscogsDumpDownloader()
    elif catalog == 'musicbrainz':
        downloader = MusicBrainzDumpDownloader()

    importer.refresh_dump(
        output, download_url, downloader)


class Importer():

    def refresh_dump(self, output_folder: str, download_url: str, downloader: BaseDumpDownloader):
        """Downloads the dump, if necessary, 
        and calls the handler over the dump file"""

        try:
            last_modified = client.http_call(download_url,
                                             'HEAD').headers[const.LAST_MODIFIED_KEY]

        except ValueError:
            last_modified = client.http_call(downloader.dump_download_url(),
                                             'HEAD').headers[const.LAST_MODIFIED_KEY]
            download_url = downloader.dump_download_url()

        last_modified = datetime.datetime.strptime(
            last_modified, '%a, %d %b %Y %H:%M:%S GMT').strftime('%Y%m%d_%H%M%S')

        extensions = download_url.split('/')[-1].split('.')[1:]

        file_name = "%s.%s" % (last_modified, '.'.join(extensions))

        file_full_path = os.path.join(output_folder, file_name)

        # checks if the current dump is up-to-date
        if not os.path.isfile(file_full_path):
            try:
                self._update_dump(download_url, file_full_path)
                try:
                    downloader.import_from_dump(file_full_path)
                except Exception as e:
                    LOGGER.warning("%s\n%s", loc.FAIL_HANDLER, str(e))
            except Exception as e:
                LOGGER.warning("%s\n%s", loc.FAIL_DOWNLOAD, str(e))

        downloader.import_from_dump(file_full_path)

    def _update_dump(self, dump_url: str, file_output_path: str) -> None:
        """Download the dump"""
        client.download_file(dump_url, file_output_path)
