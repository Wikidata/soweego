import click

from soweego.linker import linking_strategies, train

CLI_COMMANDS = {
    'baseline': linking_strategies.baseline,
    'train': train.train_cli
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
