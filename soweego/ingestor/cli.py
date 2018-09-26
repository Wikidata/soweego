#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.ingestor import wikidata_bot

CLI_COMMANDS = {
    'add_identifiers': wikidata_bot.add_identifiers_cli,
    'delete_identifiers': wikidata_bot.delete_identifiers_cli,
    'deprecate_identifiers': wikidata_bot.deprecate_identifiers_cli
}


@click.group(name='ingestor', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Ingest soweego output into Wikidata."""
    pass
