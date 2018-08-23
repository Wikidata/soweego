#!/usr/bin/env python3
# coding: utf-8

import json 
import click

import soweego.target_selection.commons.constants as constants
import soweego.target_selection.commons.utils as utils
from soweego.target_selection.commons import matching_strategies
from soweego.target_selection.bibsys.business.services.import_service import ImportService

@click.command()
@click.argument('bibsys_dictionary', default=constants.bibsys_dictionary, type=click.Path(exists
@click.argument('wikidata_samples', default=constants.wikidata_samples, type=click.Path(exists=True))=True))
@click.option('--output', '-o', default=utils.get_output_path(), type=click.Path(exists=True))
def equal_strings_match(wikidata_samples: str, bibsys_dictionary: str, output: str) -> None:
    """Creates the equal strings match output file"""
    matching_strategies.perfect_string_match_wrapper(wikidata_samples, bibsys_dictionary, output)

@click.command()
@click.argument('dump_states', default=constants.dump_states, type=click.Path(exists=True))
@click.option('--output', '-o', default=utils.get_output_path(), type=click.Path(exists=True))
def refresh_dump(dump_states: str, output: str) -> None:
    """Check if there is an updated dump in the output path; if not downloads the bibsys dump"""
    ImportService().refresh_dump(dump_states, output)