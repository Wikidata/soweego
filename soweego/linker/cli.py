import click

from soweego.linker import baseline, classify, evaluate, train

CLI_COMMANDS = {
    'baseline': baseline.cli,
    'train': train.cli,
    'classify': classify.cli,
    'evaluate': evaluate.cli,
    'extract_': baseline.extract_available_matches_in_target
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
