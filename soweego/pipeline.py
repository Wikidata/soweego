"""Run the whole soweego pipeline."""

import gc
import logging
from typing import Callable

import click
from soweego.commons import target_database
from soweego.importer.importer import import_cli
from soweego.linker import baseline, evaluate, link, train
from soweego.validator.checks import bio_cli, dead_ids_cli, links_cli

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.option(
    '--validator/--no-validator',
    default=False,
    help='Sync Wikidata to the target catalog. Default: no.',
)
@click.option(
    '--importer/--no-importer',
    default=True,
    help='Import the target catalog dump into the database. Default: yes.',
)
@click.option(
    '--linker/--no-linker',
    default=True,
    help='Link Wikidata items to target catalog identifiers. Default: yes.',
)
@click.option(
    '--upload/--no-upload',
    default=True,
    help='Upload results to Wikidata. Default: yes.',
)
def cli(
    catalog: str, validator: bool, importer: bool, linker: bool, upload: bool
):
    """Launch the whole pipeline."""

    if importer:
        _importer(catalog)
    else:
        LOGGER.info("Skipping importer")

    if linker:
        _linker(catalog, upload)
    else:
        LOGGER.info("Skipping linker")

    if validator:
        _validator(catalog, upload)
    else:
        LOGGER.info("Skipping validator")


def _importer(target: str):
    """Contains all the command the importer has to do"""
    LOGGER.info(
        "Running importer for target: %s without resolving the URLs", target
    )
    _invoke_no_exit(import_cli, [target])


def _linker(target: str, upload: bool):
    """Contains all the command the linker has to do"""
    LOGGER.info("Running linker for target: %s", target)

    for target_type in target_database.supported_entities_for_target(target):
        if not target_type:
            continue
        arguments = (
            [target, target_type, '--upload']
            if upload
            else [target, target_type]
        )

        _invoke_no_exit(baseline.extract_cli, arguments)
        _invoke_no_exit(evaluate.cli, ['slp', target, target_type])
        _invoke_no_exit(train.cli, ['slp', target, target_type])
        arg_linker = ['slp']
        arg_linker.extend(arguments)
        _invoke_no_exit(link.cli, arg_linker)


def _validator(target: str, upload: bool):
    """Contains all the command the validator has to do"""
    args = [target, '--upload'] if upload else [target]
    # Runs the validator for each kind of entity of the given target database
    for entity_type in target_database.supported_entities_for_target(target):
        args.insert(1, entity_type)
        LOGGER.info("Running validator for target %s %s", target, entity_type)
        _invoke_no_exit(dead_ids_cli, args)
        _invoke_no_exit(links_cli, args)
        _invoke_no_exit(bio_cli, args)
        args.remove(entity_type)


def _invoke_no_exit(function: Callable, args: list):
    """Given a function avoids that it exits the program"""
    try:
        function(args)
    except SystemExit:
        pass
    LOGGER.debug("GC collect %s\n" % gc.collect())
