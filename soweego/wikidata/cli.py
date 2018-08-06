import click

from soweego.wikidata import query_on_values

CLI_COMMANDS = {
    'query_on_values': query_on_values.main,
}


@click.group(name='wikidata', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Read/write operations on the knowledge base"""
    pass

