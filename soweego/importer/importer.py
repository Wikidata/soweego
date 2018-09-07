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
from soweego.importer.bibsys.bibsys_handler import handle as bibsys_handler
from soweego.importer.commons.models.dump_state import DumpState
from soweego.importer.commons.services.import_service import ImportService
from soweego.importer.musicbrainz.handler import dump_state, handler


@click.command()
@click.argument('catalog', type=click.Choice(['bibsys', 'bne', 'discogs', 'musicbrainz']))
@click.argument('dump_state_path', type=click.Path())
@click.option('--output', '-o', default='output', type=click.Path())
def import_catalog(catalog, dump_state_path: str, output: str) -> None:
    """Checks if there is an updated dump in the output path;
       if not downloads the dump"""

    import_service = ImportService()
    dump_state_dict = load(dump_state_path)

    # TODO set proper handle parameters
    if catalog == 'bibsys':
        # import_service.refresh_dump(
        #     ds, handlers.nt_handler.handle())
        print('To implement')
    elif catalog == 'bne':
        print('To implement')
    elif catalog == 'discogs':
        # import_service.refresh_dump(
        #     ds, handlers.xml_handler.handle())
        print('To implement')
    elif catalog == 'musicbrainz':
        ds = dump_state(output, dump_state_dict['last_modified'])
        ds_path = os.path.join(os.path.dirname(
            os.path.abspath(output)), 'dump_state.json')
        import_service.refresh_dump(ds_path, ds, handler)
