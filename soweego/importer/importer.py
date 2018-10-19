#!/usr/bin/env python3
# coding: utf-8

"""Click-command definitions for the importer"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import os

import click
from soweego.commons.file_utils import get_path
from soweego.commons.json_utils import load
from soweego.importer.commons.models.base_dump_download_helper import \
    BaseDumpDownloadHelper
from soweego.importer.commons.services.import_service import ImportService
from soweego.importer.musicbrainz.muscbrainz_dump_download_helper import \
    MusicbrainzDumpDownloadHelper


@click.command()
@click.argument('catalog', type=click.Choice(['bne', 'discogs', 'musicbrainz']))
@click.option('--output', '-o', default='output', type=click.Path())
@click.option('--download-uri', '-dp', default=None)
def import_catalog(catalog, output: str, download_uri: str) -> None:
    """Checks if there is an updated dump in the output path;
       if not downloads the dump"""

    import_service = ImportService()
    download_helper = BaseDumpDownloadHelper()

    if catalog == 'bne':
        raise NotImplementedError
    elif catalog == 'discogs':
        # TODO implement a DownloadHelper for Discogs. There's already an old style handler written
        download_helper = BaseDumpDownloadHelper()
    elif catalog == 'musicbrainz':
        download_helper = MusicbrainzDumpDownloadHelper()

    import_service.refresh_dump(
        output, download_uri, download_helper)


# Se nome dump calcolato esiste già, non fare nulla a meno di opzione -f
# Scarico un dump da SITO CALCOLATO su base di NOME DB FORNITO
# lo metto in POSTO SPECIFICATO con nome CATALOGO_DATAULTIMAMODIFICAINSECONDIDAL1970.ESTENSIONEFONRITA
