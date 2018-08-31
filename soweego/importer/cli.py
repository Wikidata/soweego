#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

import click

from soweego.importer import importer as importer

CLI_COMMANDS = {
    'import': importer.refresh_dumps,
}

@click.group(name='importer', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Imports dumps into the DB."""
    pass
