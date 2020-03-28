#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""The command line interface entry point."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click

from soweego import commons
from soweego import pipeline as pipeline_cli
from soweego.importer import cli as importer_cli
from soweego.ingester import cli as ingester_cli
from soweego.linker import cli as linker_cli
from soweego.validator import cli as validator_cli

# Silence requests log up to INFO
logging.getLogger('requests').setLevel(logging.WARNING)

# Silence tensorflow, see https://tinyurl.com/qnud7j8
# Python log up to WARNING
logging.getLogger('tensorflow').setLevel(logging.ERROR)
# C++ log up to W(arning)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Silence ML-Ensemble, see http://ml-ensemble.com/docs/config.html
os.environ['MLENS_VERBOSE'] = '0'


CLI_COMMANDS = {
    'importer': importer_cli.cli,
    'ingester': ingester_cli.cli,
    'linker': linker_cli.cli,
    'sync': validator_cli.cli,
    'run': pipeline_cli.cli,
}


@click.group(commands=CLI_COMMANDS)
@click.option(
    '-l',
    '--log-level',
    type=(str, click.Choice(commons.logging.LEVELS)),
    multiple=True,
    help=(
        'Module name followed by one of '
        '[DEBUG, INFO, WARNING, ERROR, CRITICAL]. '
        'Multiple pairs allowed.'
    ),
)
@click.pass_context
def cli(ctx, log_level):
    """Link Wikidata to large catalogs."""
    commons.logging.setup()
    for module, level in log_level:
        commons.logging.set_log_level(module, level)
