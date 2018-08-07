import click

from soweego.target_selection.discogs import baseline_matcher

CLI_COMMANDS = {
    'baseline_matcher': baseline_matcher.main,
}


@click.group(name='discogs', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over Discogs, a music database"""
    pass
