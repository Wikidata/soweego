import logging

import click
from soweego.commons import target_database
from soweego.commons.db_manager import DBManager
from soweego.importer.importer import import_cli, validate_links_cli
from soweego.linker import baseline, evaluate, train, classify
from soweego.validator.checks import (check_existence_cli, check_links_cli,
                                      check_metadata_cli)

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.option('--validator/--no-validator', default=False,
              help='Executes the validation steps for the target. Default: no.')
@click.option('--importer/--no-importer', default=True,
              help='Executes the importer steps for the target. Default: yes.')
@click.option('--linker/--no-linker', default=True, help='Executes the linker steps for the target. Default: yes.')
@click.option('--upload/--no-upload', default=True, help='Upload the results on wikidata. Default: yes.')
@click.option('-c', '--credentials-path', type=click.Path(file_okay=True), default=None,
              help="default: None")
def cli(target: str, validator: bool, importer: bool, linker: bool, upload: bool, credentials_path: str):
    """Executes importer/linker and optionally validator for a target"""

    if credentials_path:
        LOGGER.info("Using database credentials from file %s" %
                    credentials_path)
        DBManager.set_credentials_from_path(credentials_path)

    if importer:
        _importer(target)
    else:
        LOGGER.info("Skipping importer")

    if linker:
        _linker(target, upload)
    else:
        LOGGER.info("Skipping linker")

    if validator:
        _validator(target, upload)
    else:
        LOGGER.info("Skipping validator")


def _importer(target: str):
    LOGGER.info("Running importer for target: %s without resolving the URLs" % target)
    import_cli([target, '--no-resolve'])
    LOGGER.info("Validating URL resolving them for target %s" % target)
    validate_links_cli([target])


def _linker(target: str, upload: bool):
    LOGGER.info("Running linker for target: %s" % target)
    upload_option = "--upload" if upload else "--no-upload"
    for target_type in target_database.available_types_for_target(target):
        baseline.cli([target, target_type, '-s', 'all', upload_option])
        evaluate.cli(['nb', target, target_type])
        train.cli(['nb', target, target_type])
        classify.cli([target, target_type, '/app/shared/musicbrainz_%s_nb_model.pkl'.format(target_type), upload_option])


def _validator(target: str, upload: bool):
    upload_option = "--upload" if upload else "--no-upload"
    # Runs the validator for each kind of entity of the given target database
    for entity_type in target_database.available_types_for_target(target):
        LOGGER.info("Running validator for target %s %s" %
                    (target, entity_type))
        check_existence_cli([entity_type, target, upload_option])
        check_links_cli([entity_type, target, upload_option])
        check_metadata_cli([entity_type, target, upload_option])
