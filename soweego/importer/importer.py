#!/usr/bin/env python3
# coding: utf-8

import click

import soweego.importer.constants as constants 
from soweego.importer.services.import_service import ImportService

@click.command()
@click.argument('dump_states', default=constants.DUMP_STATES, type=click.Path(exists=True))
@click.option('--output', '-o', default='TODO output path', type=click.Path(exists=True))
def refresh_dumps(dump_states: str, output: str) -> None:
    """Checks if there is an updated dump in the output path; if not downloads the bibsys dump"""
    ImportService().refresh_dumps(dump_states, output)