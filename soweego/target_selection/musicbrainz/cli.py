import click
from soweego.target_selection.musicbrainz import musicbrainz_baseline_matcher

CLI_COMMANDS = {
    'get_users_urls': musicbrainz_baseline_matcher.get_users_urls,
    'links_match': musicbrainz_baseline_matcher.links_match,
    'get_url_domains': musicbrainz_baseline_matcher.get_url_domains,
    'get_label_musicbrainzid_dict': musicbrainz_baseline_matcher.get_label_musicbrainzid_dict,
    'get_users_label_dates_dictionary': musicbrainz_baseline_matcher.get_users_label_dates_dictionary
}


@click.group(name='musicbrainz', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Operations over this target"""
    pass
