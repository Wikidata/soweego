import click
from soweego.target_selection import common, discogs, musicbrainz

CLI_COMMANDS = {
    'musicbrainz': musicbrainz.cli.cli,
    'discogs': discogs.cli.cli,
    'common': common.cli.cli
}


@click.group(commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Investigation on candidate targets"""
    pass
