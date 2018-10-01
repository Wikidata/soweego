#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.validator import checks

CLI_COMMANDS = {
    'check_existence': checks.check_existence_cli
}


@click.group(name='validator', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Sanity checks of existing identifiers in Wikidata."""
    pass
