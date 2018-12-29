import click
from soweego.linker import linking_strategies

CLI_COMMANDS = {
    'baseline': linking_strategies.baseline
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
