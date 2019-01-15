import click
from soweego.commons import target_database


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.option('--validator/--no-validator', default=False, help='Executes the validation steps for the target. Default: no.')
@click.option('-c', '--credentials-path', type=click.Path(file_okay=True), default=None,
              help="default: None")
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared',
              help="default: '/app/shared'")
def pipeline(target: str, validator: bool, credentials_path: str, output_dir: str):
    """Executes importer/linker and optionally validator for a target"""

    if credentials_path:
        DBManager.set_credentials_from_path(credentials_path)

    _importer(target, output_dir)
    _linker(target, output_dir)
    if validator:
        _validator(target, output_dir)


def _importer(target: str, output_dir: str):
    return


def _linker(target: str, output_dir: str):
    return


def _validator(target: str, output_dir: str):
    return


# Calls click commands
# Copied from https://stackoverflow.com/questions/48619517/call-a-click-command-from-code
def call_click_command(cmd, *args, **kwargs):
    """ Wrapper to call a click command

    :param cmd: click cli command function to call 
    :param args: arguments to pass to the function 
    :param kwargs: keywrod arguments to pass to the function 
    :return: None 
    """

    # Get positional arguments from args
    arg_values = {c.name: a for a, c in zip(args, cmd.params)}
    args_needed = {c.name: c for c in cmd.params
                   if c.name not in arg_values}

    # build and check opts list from kwargs
    opts = {a.name: a for a in cmd.params if isinstance(a, click.Option)}
    for name in kwargs:
        if name in opts:
            arg_values[name] = kwargs[name]
        else:
            if name in args_needed:
                arg_values[name] = kwargs[name]
                del args_needed[name]
            else:
                raise click.BadParameter(
                    "Unknown keyword argument '{}'".format(name))

    # check positional arguments list
    for arg in (a for a in cmd.params if isinstance(a, click.Argument)):
        if arg.name not in arg_values:
            raise click.BadParameter("Missing required positional"
                                     "parameter '{}'".format(arg.name))

    # build parameter lists
    opts_list = sum(
        [[o.opts[0], str(arg_values[n])] for n, o in opts.items()], [])
    args_list = [str(v) for n, v in arg_values.items() if n not in opts]

    # call the command
    cmd(opts_list + args_list)
