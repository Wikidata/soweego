#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.commons import candidate_acquisition

CLI_COMMANDS = {
    'build_index': candidate_acquisition.build_index,
    'drop_index': candidate_acquisition.drop_index,
    'query_index': candidate_acquisition.query_index
}


@click.group(name='commons', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Common utilities."""
    pass
