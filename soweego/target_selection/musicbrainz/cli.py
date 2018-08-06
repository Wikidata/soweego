import click

from soweego.target_selection.musicbrainz import musicbrainz_baseline_matcher
from soweego.target_selection.musicbrainz import wikidata_sample_additional_info

CLI_COMMANDS = {
    'baseline_matcher': musicbrainz_baseline_matcher.equal_strings_match,
    'get_sample_links': wikidata_sample_additional_info.get_wikidata_sample_links,
    'get_users_urls': musicbrainz_baseline_matcher.get_users_urls
}


@click.group(name='musicbrainz', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over this target"""
    pass

