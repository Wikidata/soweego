#!/usr/bin/env python3
# coding: utf-8

import logging
import click

from soweego import commons, wikidata, target_selection

CLI_COMMANDS = {
    'wikidata': wikidata.cli.cli,
    'target_selection': target_selection.cli.cli,
}

# Avoid verbose requests logging
logging.getLogger("requests").setLevel(logging.WARNING)


@click.group(commands=CLI_COMMANDS)
@click.option('--log-level',
              type=(str, click.Choice(commons.logging.LEVELS)),
              multiple=True,
              help='Module name followed by one of [DEBUG, INFO, WARNING, ERROR, CRITICAL].')
@click.pass_context
def cli(ctx, log_level):
    """Link Wikidata items to trusted external catalogs."""
    commons.logging.setup()
    for module, level in log_level:
        commons.logging.set_log_level(module, level)
