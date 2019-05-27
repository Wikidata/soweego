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

from soweego import commons
from soweego import pipeline as pipeline_cli
from soweego import wikidata
from soweego.importer import cli as importer_cli
from soweego.ingestor import cli as ingestor_cli
from soweego.linker import cli as linker_cli
from soweego.validator import cli as validator_cli
from soweego.wikidata import cli as wikidata_cli

CLI_COMMANDS = {
    'importer': importer_cli.cli,
    'ingestor': ingestor_cli.cli,
    'linker': linker_cli.cli,
    'validator': validator_cli.cli,
    'wikidata': wikidata_cli.cli,
    'run': pipeline_cli.cli,
}

# Avoid verbose requests logging
logging.getLogger("requests").setLevel(logging.WARNING)


@click.group(commands=CLI_COMMANDS)
@click.option(
    '-l',
    '--log-level',
    type=(str, click.Choice(commons.logging.LEVELS)),
    multiple=True,
    help='Module name followed by one of [DEBUG, INFO, WARNING, ERROR, CRITICAL]. Multiple pairs allowed.',
)
@click.pass_context
def cli(ctx, log_level):
    """Link Wikidata items to trusted external catalogs."""

    commons.logging.setup()
    for module, level in log_level:
        commons.logging.set_log_level(module, level)

    # setup bot authentication
    wikidata.api_requests.get_authenticated_session()
