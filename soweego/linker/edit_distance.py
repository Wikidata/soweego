import gzip
import json
import logging
from collections import defaultdict
from os import path
from typing import Iterable, Tuple

import click
import jellyfish
from soweego.commons import data_gathering, target_database, text_utils, constants
from soweego.importer.models.base_entity import BaseEntity
from soweego.ingestor import wikidata_bot
from soweego.wikidata.api_requests import get_data_for_linker

EDIT_DISTANCES = {
    'jw': jellyfish.jaro_winkler,
    'l': jellyfish.levenshtein_distance,
    'dl': jellyfish.damerau_levenshtein_distance
}
LOGGER = logging.getLogger(__name__)
WD_IO_FILENAME = 'wikidata_%s_dataset.jsonl.gz'


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-s', '--strategy', type=click.Choice(EDIT_DISTANCES.keys()), default='jw')
@click.option('--upload/--no-upload', default=False, help='Upload check results to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('--threshold', '-t', default=0, type=float, help="Edit distances with a higher value than this are treated as matches.")
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="default: '%s'" % constants.SHARED_FOLDER)
def cli(target, target_type, strategy, upload, sandbox, threshold, output_dir):
    # Wikidata
    wd_io_path = path.join(output_dir, WD_IO_FILENAME % target)
    if not path.exists(wd_io_path):
        qids = data_gathering.gather_qids(
            target_type, target, target_database.get_pid(target))
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(target, qids, url_pids, ext_id_pids_to_urls, wd_io)
            LOGGER.info("Wikidata stream stored in %s" % wd_io_path)

    target_entity = target_database.get_entity(target, target_type)
    target_pid = target_database.get_pid(target)
    with gzip.open(wd_io_path, "rt") as wd_io:
        result = edit_distance_match(
            wd_io, target_entity, target_pid, strategy, threshold)
        if upload:
            wikidata_bot.add_statements(
                result, target_database.get_qid(target), sandbox)
        else:
            filepath = path.join(output_dir, 'edit_distance_%s.csv' % strategy)
            with open(filepath, 'w') as filehandle:
                for res in result:
                    res_to_string = [str(r) for r in res]
                    filehandle.write('%s\n' % ";".join(res_to_string))
                    filehandle.flush()
            LOGGER.info('Edit distance %s strategy against %s dumped to %s',
                        strategy, target, filepath)


def edit_distance_match(source, target: BaseEntity, target_pid: str, metric: str, threshold: float) -> Iterable[Tuple[str, str, str, float]]:
    """Given a source dataset ``{identifier: {string: [languages]}}``,
    match strings having the given edit distance ``metric``
    above the given ``threshold`` and return a dataset
    ``[(source_id, PID, target_id, distance_score), ...]``.

    Compute the distance for each ``(source, target)`` entity pair.
    Target candidates are acquired as follows:
    - build a query upon the most frequent source entity strings;
    - exact strings are joined in an OR query, e.g., ``"string1" "string2"``;
    - run the query against a database table containing indexed of target entities.

    ``distance_type`` can be one of:

    - ``jw``, `Jaro-Winkler <https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance>`_;
    - ``l``, `Levenshtein<https://en.wikipedia.org/wiki/Levenshtein_distance>`_;
    - ``dl``, `Damerau-Levenshtein<https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance>`_.

    Return ``None`` if the given edit distance is not valid.
    """
    distance_function = EDIT_DISTANCES.get(metric)
    if not distance_function:
        LOGGER.error(
            'Invalid distance_type parameter: "%s". ' +
            'Please pick one of "jw" (Jaro-Winkler), "l" (Levenshtein) ' +
            'or "dl" (Damerau-Levenshtein)', metric)
        return None
    LOGGER.info('Using %s edit distance', distance_function.__name__)
    for entity_row in source:
        entity = json.loads(entity_row)
        source_id, source_strings = entity['qid'], entity['label']
        query, most_frequent_source_strings = _build_index_query(
            source_strings)
        LOGGER.debug('Query: %s', query)
        target_candidates = data_gathering.name_fulltext_search(
            target, query)
        if target_candidates is None:
            LOGGER.warning('Skipping query that went wrong: %s', query)
            continue
        if target_candidates == {}:
            LOGGER.info('Skipping query with no results: %s', query)
            continue
        # This should be a very small loop, just 1 iteration most of the time
        for source_string in most_frequent_source_strings:
            source_ascii, source_normalized = text_utils.normalize(
                source_string)
            for result in target_candidates:
                target_string = result.name
                target_id = result.catalog_id
                target_ascii, target_normalized = text_utils.normalize(
                    target_string)
                try:
                    distance = distance_function(
                        source_normalized, target_normalized)
                # Damerau-Levenshtein does not support some Unicode code points
                except ValueError:
                    LOGGER.warning(
                        'Skipping unsupported string in pair: "%s", "%s"',
                        source_normalized, target_normalized)
                    continue
                LOGGER.debug('Source: %s > %s > %s - Target: %s > %s > %s - Distance: %f',
                             source_string, source_ascii, source_normalized,
                             target_string, target_ascii, target_normalized,
                             distance)
                if (metric in ('l', 'dl') and distance <= threshold) or (metric == 'jw' and distance >= threshold):
                    yield (source_id, target_pid, target_id, distance)
                    LOGGER.debug("It's a match! %s -> %s",
                                 source_id, target_id)
                else:
                    LOGGER.debug('Skipping potential match due to the threshold: %s -> %s - Threshold: %f - Distance: %f',
                                 source_id, target_id, threshold, distance)


def _build_index_query(source_strings):
    query_builder = []
    frequencies = defaultdict(list)
    for label, languages in source_strings.items():
        frequencies[len(languages)].append(label)
    most_frequent = frequencies[max(frequencies.keys())]
    for label in most_frequent:
        # TODO experiment with different strategies
        query_builder.append('"%s"' % label)
    return ' '.join(query_builder), most_frequent
