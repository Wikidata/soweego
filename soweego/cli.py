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
import tensorflow as tf

from soweego import commons
from soweego import pipeline as pipeline_cli
from soweego.importer import cli as importer_cli
from soweego.ingester import cli as ingester_cli
from soweego.linker import cli as linker_cli
from soweego.validator import cli as validator_cli

# set env variable to ignore tensorflow warnings
# (only errors are printed)
tf.get_logger().setLevel(logging.ERROR)


CLI_COMMANDS = {
    'importer': importer_cli.cli,
    'ingester': ingester_cli.cli,
    'linker': linker_cli.cli,
    'sync': validator_cli.cli,
    'run': pipeline_cli.cli,
}

# Avoid verbose requests logging
logging.getLogger('requests').setLevel(logging.WARNING)


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
