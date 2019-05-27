#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Click-command descriptions for the importer"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.importer.importer import import_cli, check_links_cli

CLI_COMMANDS = {
    'import': import_cli,
    'check_links': check_links_cli
}


@click.group(name='importer', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Import target dumps into the database."""
