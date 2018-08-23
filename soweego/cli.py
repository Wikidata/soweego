#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

import click

from soweego import commons, target_selection, wikidata

CLI_COMMANDS = {
    'commons': commons.cli.cli,
    'wikidata': wikidata.cli.cli,
    'target_selection': target_selection.cli.cli,
}

# Avoid verbose requests logging
logging.getLogger("requests").setLevel(logging.WARNING)


@click.group(commands=CLI_COMMANDS)
@click.option('--log-level',
              type=(str, click.Choice(commons.logging.LEVELS)),
              multiple=True,
              help='Module name followed by one of [DEBUG, INFO, WARNING, ERROR, CRITICAL].')
@click.pass_context
def cli(ctx, log_level):
    """Link Wikidata items to trusted external catalogs."""
    commons.logging.setup()
    for module, level in log_level:
        commons.logging.set_log_level(module, level)
