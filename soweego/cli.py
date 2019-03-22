#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""The command line interface entry point."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

import click
from soweego import (commons, importer, ingestor, linker, pipeline, validator,
                     wikidata)
                     
CLI_COMMANDS = {
    'commons': commons.cli.cli,
    'importer': importer.cli.cli,
    'ingestor': ingestor.cli.cli,
    'linker': linker.cli.cli,
    'validator': validator.cli.cli,
    'wikidata': wikidata.cli.cli,
    'run': pipeline.cli
}

# Avoid verbose requests logging
logging.getLogger("requests").setLevel(logging.WARNING)


@click.group(commands=CLI_COMMANDS)
@click.option('-l', '--log-level',
              type=(str, click.Choice(commons.logging.LEVELS)),
              multiple=True,
              help='Module name followed by one of [DEBUG, INFO, WARNING, ERROR, CRITICAL]. Multiple pairs allowed.')
@click.pass_context
def cli(ctx, log_level):
    """Link Wikidata items to trusted external catalogs."""

    # setup bot authentication
    wikidata.api_requests.get_authenticated_session()
    
    commons.logging.setup()
    for module, level in log_level:
        commons.logging.set_log_level(module, level)
