#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.target_selection.commons import matching_strategies

CLI_COMMANDS = {
    'perfect_strings_match': matching_strategies.perfect_string_match_cli
}


@click.group(name='commons', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Shared operations."""
    pass
