#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Click-command descriptions for the importer"""

import click

from soweego.importer import importer as importer

CLI_COMMANDS = {
    'import_bibsys': importer.import_bibsys,
}

@click.group(name='importer', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Imports dumps into the DB."""
    pass
