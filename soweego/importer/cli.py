#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Click-command descriptions for the importer"""

import click
from soweego.importer import importer

CLI_COMMANDS = {
    'import_catalog': importer.import_catalog,
}


@click.group(name='importer', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Imports dumps into the DB."""
    pass
