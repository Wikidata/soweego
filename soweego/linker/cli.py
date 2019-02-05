import click
from soweego.linker import baseline, edit_distance

CLI_COMMANDS = {
    'baseline': baseline.cli,
    'edit-distance': edit_distance.cli
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
