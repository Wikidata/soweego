#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'


import click

from soweego.wikidata import query_on_values, sample_additional_info, sparql_queries

CLI_COMMANDS = {
    'sparql_queries': sparql_queries.values_query,
    'get_sitelinks_for_sample': sample_additional_info.get_sitelinks_for_sample,
    'get_links_for_sample': sample_additional_info.get_links_for_sample,
    'get_birth_death_dates_for_sample':
        sample_additional_info.get_birth_death_dates_for_sample,
    'get_url_formatters_for_properties':
        sample_additional_info.get_url_formatters_for_properties
}


@click.group(name='wikidata', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Read/write operations on Wikidata."""
    pass
