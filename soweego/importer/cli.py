#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Importer CLI commands."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import click
from soweego.importer.importer import check_urls_cli, import_cli

CLI_COMMANDS = {'import': import_cli, 'check_urls': check_urls_cli}


@click.group(name='importer', commands=CLI_COMMANDS)
@click.pass_context
def cli(_):
    """Import target catalog dumps into a SQL database."""
