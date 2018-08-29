import click
from soweego.target_selection.common import matching_strategies_wrapper

CLI_COMMANDS = {
    'perfect_strings_match': matching_strategies_wrapper.perfect_string_match
}


@click.group(name='common', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over this target"""
    pass
