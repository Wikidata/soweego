import click
from soweego.linker import baseline

CLI_COMMANDS = {
    'baseline': baseline.baseline
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
