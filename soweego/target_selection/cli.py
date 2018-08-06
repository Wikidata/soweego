import click

from soweego.target_selection import musicbrainz

CLI_COMMANDS = {
    'musicbrainz': musicbrainz.cli.cli
}

@click.group(commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Investigation on candidate targets"""
    pass

