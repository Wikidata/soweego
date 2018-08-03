import click

from soweego.target_selection.musicbrainz import musicbrainz_baseline_matcher

CLI_COMMANDS = {
    'baseline_matcher': musicbrainz_baseline_matcher.equal_strings_match,
}


@click.group(name='musicbrainz', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over this target"""
    pass

