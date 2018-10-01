#!/usr/bin/env python3
# coding: utf-8

"""Click-command definitions for the Bibsys baseline-matcher"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import click

from soweego.commons.file_utils import get_path
from soweego.commons.json_utils import load
from soweego.importer.commons.models.dump_state import DumpState
from soweego.importer.commons.services.import_service import ImportService
from soweego.target_selection.bibsys import bibsys_baseline_helper as helper
from soweego.target_selection.commons import constants, matching_strategies


@click.command()
@click.argument('matcher', type=click.Choice(['name', 'link']))
@click.option('-d', '--dump-state-path', default=get_path("soweego.importer.bibsys.resources", "dump_state.json"), type=click.Path(exists=True))
@click.option('-o', '--output', default=get_path("soweego.importer.bibsys.output", "bibsys.nt"), type=click.Path())
def download_and_scrape(matcher: str, dump_state_path: str, output: str) -> None:
    """Downloads the dump and generate a .json file for the matcher"""
    dictionary = load(dump_state_path)
    dump_state = DumpState(
        output, dictionary['download_url'], dictionary['last_modified'])
    if matcher == 'name':
        ImportService().refresh_dump(dump_state_path, dump_state, helper.name_scraper)
    elif matcher == 'link':
        # TODO implement helper.link_scraper
        raise NotImplementedError


@click.command()
@click.argument('bibsys_dictionary', default=constants.BIBSYS_DICTIONARY, type=click.Path(exists=True))
@click.argument('wikidata_samples', default=constants.WIKIDATA_SAMPLES, type=click.Path(exists=True))
@click.option('-o', '--output', default='output/bibsys_perfect_matches.json', type=click.Path())
def equal_strings_match(wikidata_samples: str, bibsys_dictionary: str, output: str) -> None:
    """Creates the equal strings match output file"""
    matching_strategies.perfect_string_match_wrapper(
        wikidata_samples, bibsys_dictionary, output)
