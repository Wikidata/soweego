import click

from soweego.linker import baseline, link, evaluate, train

CLI_COMMANDS = {
    'baseline': baseline.cli,
    'evaluate': evaluate.cli,
    'extract': baseline.extract_cli,
    'link': link.cli,
    'train': train.cli,
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
