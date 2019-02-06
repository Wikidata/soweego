import click

from soweego.linker import baseline, classify, edit_distance, evaluate, train

CLI_COMMANDS = {
    'baseline': baseline.cli,
    'edit-distance': edit_distance.cli,
    'train': train.cli,
    'classify': classify.cli,
    'evaluate': evaluate.cli


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
