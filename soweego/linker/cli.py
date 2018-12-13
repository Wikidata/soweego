import click
from soweego.linker import linking_strategies, musicbrainz_baseline

CLI_COMMANDS = {
    'get_users_urls': musicbrainz_baseline.get_users_urls,
    'links_match': musicbrainz_baseline.links_match,
    'get_url_domains': musicbrainz_baseline.get_url_domains,
    'get_label_musicbrainzid_dict': musicbrainz_baseline.get_label_musicbrainzid_dict,
    'get_users_label_dates_dictionary': musicbrainz_baseline.get_users_label_dates_dictionary,
    'baseline': linking_strategies.baseline
}


@click.group(name='linker', commands=CLI_COMMANDS)
@click.pass_context
def cli(ctx):
    """Link Wikidata items to target catalog identifiers."""
    pass
