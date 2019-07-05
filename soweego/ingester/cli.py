#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Ingestor CLI commands."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.ingester import mix_n_match_client, wikidata_bot

CLI_COMMANDS = {
    'delete': wikidata_bot.delete_cli,
    'deprecate': wikidata_bot.deprecate_cli,
    'identifiers': wikidata_bot.identifiers_cli,
    'mnm': mix_n_match_client.cli,
    'people': wikidata_bot.people_cli,
    'works': wikidata_bot.works_cli,
}


@click.group(name='ingest', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Take soweego output into Wikidata items."""
