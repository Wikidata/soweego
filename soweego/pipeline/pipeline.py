import click
from soweego.commons import target_database


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.option('--validator/--no-validator', default=False, help='Executes the validation steps for the target. Default: no.')
@click.option('-c', '--credentials-path', type=click.Path(file_okay=True), default=None,
              help="default: None")
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared',
              help="default: '/app/shared'")
def pipeline(target: str, validator: bool, credentials_path: str, output_dir: str):
    """Executes importer/linker and optionally validator for a target"""

    if credentials_path:
        DBManager.set_credentials_from_path(credentials_path)

    _importer(target, output_dir)
    _linker(target, output_dir)
    if validator:
        _validator(target, output_dir)


def _importer(target: str, output_dir: str):
    return


def _linker(target: str, output_dir: str):
    return


def _validator(target: str, output_dir: str):
    return
