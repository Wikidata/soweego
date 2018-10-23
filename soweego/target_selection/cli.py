#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import click
from soweego.target_selection import commons, discogs, musicbrainz

CLI_COMMANDS = {
    'commons': commons.cli.cli,
    'discogs': discogs.cli.cli,
    'musicbrainz': musicbrainz.cli.cli
}


@click.group(name='target_selection', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Investigation on candidate targets."""
    pass
