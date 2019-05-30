#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Ingestor CLI commands."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.ingestor import mix_n_match_client, wikidata_bot

CLI_COMMANDS = {
    'add': wikidata_bot.add_cli,
    'add_people_statements': wikidata_bot.add_people_statements_cli,
    'add_works_statements': wikidata_bot.add_works_statements_cli,
    'delete': wikidata_bot.delete_cli,
    'deprecate': wikidata_bot.deprecate_cli,
    'mix_n_match': mix_n_match_client.cli,
}


@click.group(name='ingestor', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Ingest soweego output into Wikidata."""
