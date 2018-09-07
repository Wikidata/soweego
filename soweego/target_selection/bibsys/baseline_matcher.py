#!/usr/bin/env python3
# coding: utf-8

import click
from soweego.target_selection.commons import constants, matching_strategies


@click.command()
@click.argument('bibsys_dictionary', default=constants.BIBSYS_DICTIONARY, type=click.Path(exists=True))
@click.argument('wikidata_samples', default=constants.WIKIDATA_SAMPLES, type=click.Path(exists=True))
@click.option('--output', '-o', default='TODO output path', type=click.Path(exists=True))
def equal_strings_match(wikidata_samples: str, bibsys_dictionary: str, output: str) -> None:
    """Creates the equal strings match output file"""
    matching_strategies.perfect_string_match_wrapper(
        wikidata_samples, bibsys_dictionary, output)
