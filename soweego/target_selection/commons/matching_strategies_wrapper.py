import json
import os

import click
from . import matching_strategies #TODO revert


@click.command()
@click.argument('jsons', type=click.Path(exists=True), nargs=-1)
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def perfect_string_match(jsons, output):
    path = os.path.join(output, 'matches.json')
    dictionaries = [json.load(open(j)) for j in jsons]
    matches = matching_strategies.perfect_string_match(dictionaries)
    json.dump(matches, open(path,
                            'w'), indent=2, ensure_ascii=False)
