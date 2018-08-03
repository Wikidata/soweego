import click

from soweego import wikidata, target_selection

CLI_COMMANDS = {
    'wikidata': wikidata.cli.cli,
    'target_selection': target_selection.cli.cli,
}

@click.group(commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    pass

