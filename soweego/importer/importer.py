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
from soweego.importer.bibsys.bibsys_handler import handle as bibsys_handler
from soweego.importer.commons.models.dump_state import DumpState
from soweego.importer.commons.services.import_service import ImportService


@click.command()
@click.argument('catalogs', type=click.Choice(['bibsys', 'bne', 'discogs', 'musicbrainz'], multiple=True))
@click.argument('dump_state_path', default=get_path("soweego.importer.bibsys.resources", "dump_state.json"), type=click.Path(exists=True))
@click.option('--output', '-o', default=get_path("soweego.importer.bibsys.output", "bibsys.nt"), type=click.Path(exists=True))
def import_bibsys(dump_state_path: str, output: str) -> None:
    """Checks if there is an updated dump in the output path;
       if not downloads the bibsys dump"""
    import_service = ImportService()
    dictionary = load(dump_state_path)
    dump_state = DumpState(
        output, dictionary['download_url'], dictionary['last_modified'])
    # TODO set proper handle parameters
    for catalog in catalogs:
        if catalog == 'bibsys':
            import_service.refresh_dump(
                dump_state, handlers.nt_handler.handle())
        elif catalog == 'bne':
        elif catalog == 'discogs':
            import_service.refresh_dump(
                dump_state, handlers.xml_handler.handle())
        elif catalog == 'musicbrainz':
            import_service.refresh_dump(
                dump_state, handlers.csv_handler.handle())
