#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Validator CLI commands."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click

from soweego.validator import checks, enrichment

CLI_COMMANDS = {
    'ids': checks.dead_ids_cli,
    'links': checks.links_cli,
    'bio': checks.bio_cli,
    'works': enrichment.works_people_cli,
}


@click.group(name='sync', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Sync Wikidata to target catalogs."""
