import click
from soweego.commons import target_database
from soweego.commons.db_manager import DBManager
from soweego.importer.importer import import_cli
from soweego.validator.checks import (check_existence_cli, check_links_cli,
                                      check_metadata_cli)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.option('--validator/--no-validator', default=False, help='Executes the validation steps for the target. Default: no.')
@click.option('--importer/--no-importer', default=True, help='Executes the importer steps for the target. Default: yes.')
@click.option('--linker/--no-linker', default=True, help='Executes the linker steps for the target. Default: yes.')
@click.option('--upload/--no-upload', default=True, help='Upload the results on wikidata. Default: yes.')
@click.option('-c', '--credentials-path', type=click.Path(file_okay=True), default=None,
              help="default: None")
def pipeline(target: str, validator: bool, importer: bool, linker: bool, upload: bool, credentials_path: str):
    """Executes importer/linker and optionally validator for a target"""

    if credentials_path:
        DBManager.set_credentials_from_path(credentials_path)

    if importer:
        _importer(target)
    if linker:
        _linker(target)
    if validator:
        _validator(target, upload)


def _importer(target: str):
    import_cli([target])


def _linker(target: str):
    return


def _validator(target: str, upload: bool):
    upload_option = "--upload" if upload else "--no-upload"
    # Runs the validator for each kind of entity of the given target database
    for entity_type in target_database.available_types_for_target(target):
        check_existence_cli([entity_type, target, upload_option])
        check_links_cli([entity_type, target, upload_option])
        check_metadata_cli([entity_type, target, upload_option])
