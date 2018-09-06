#!/usr/bin/env python3
# coding: utf-8

"""Click-command definitions for the importer"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import click
from soweego.commons.file_utils import get_path
from soweego.commons.json_utils import load
from soweego.importer.commons.models.dump_state import DumpState
from soweego.importer.commons.services.import_service import ImportService
from soweego.importer.bibsys.bibsys_handler import handle as bibsys_handler
from soweego.importer.musicbrainz.handler import handler as musicbrainz_handler
from soweego.importer.discogs.discogs_handler import handle as discogs_handler


@click.command()
@click.argument('catalog', type=click.Choice(['bibsys', 'bne', 'discogs', 'musicbrainz']))
@click.argument('dump_state_path', default=get_path("soweego.importer.bibsys.resources", "dump_state.json"), type=click.Path(exists=True))
@click.option('--output', '-o', default=get_path("soweego.importer.bibsys.output", "bibsys.nt"), type=click.Path(exists=True))
def import_bibsys(catalog, dump_state_path: str, output: str) -> None:
    """Checks if there is an updated dump in the output path;
       if not downloads the bibsys dump"""
    import_service = ImportService()
    dictionary = load(dump_state_path)
    #TODO choose the right dump_state.json and output path
    dump_state = DumpState(
        output, dictionary['download_url'], dictionary['last_modified'])
    # TODO set proper handle parameters
    if catalog == 'bibsys':
        import_service.refresh_dump(
            dump_state, bibsys_handler)
    elif catalog == 'bne':
        print('To implement')
    elif catalog == 'discogs':
        import_service.refresh_dump(
            dump_state, discogs_handler)
    elif catalog == 'musicbrainz':
        import_service.refresh_dump(
            dump_state, musicbrainz_handler)
