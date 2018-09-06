import click

from soweego.target_selection.bibsys import bibsys_baseline_matcher

CLI_COMMANDS = {
    'baseline_matcher': bibsys_baseline_matcher.equal_strings_match
}

@click.group(name='bibsys', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over bibsys target"""
    pass
