#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Training set construction for supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import logging
import os

import click
import recordlinkage as rl
from pandas import read_json
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, target_database
from soweego.linker import workflow
from soweego.wikidata.api_requests import get_data_for_linker

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-b', '--binarize', default=0.1, help="Default: 0.1")
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared', help="Default: '/app/shared'")
def cli(classifier, target, target_type, binarize, output_dir):
    """Train a supervised classifier for probabilistic linking."""

    model = execute(
        constants.CLASSIFIERS[classifier], target, target_type, binarize, output_dir)
    outfile = os.path.join(
        output_dir, constants.LINKER_MODEL % (target, classifier))
    joblib.dump(model, outfile)
    LOGGER.info("%s model dumped to '%s'", classifier, outfile)


def execute(classifier, catalog, entity, binarize, dir_io):
    wd_reader, target_reader = _build(catalog, entity, dir_io)
    wd, target = workflow.preprocess(wd_reader, target_reader)
    candidate_pairs = _block(wd, target)
    feature_vectors = workflow.extract_features(candidate_pairs, wd, target)
    return _train(classifier, feature_vectors, candidate_pairs, binarize)


def _train(classifier, feature_vectors, candidate_pairs, binarize):
    # TODO expose other useful parameters
    if classifier is rl.NaiveBayesClassifier:
        model = classifier(binarize=binarize)
    elif classifier is rl.SVMClassifier:
        # TODO implement SVM
        raise NotImplementedError
    LOGGER.info('Training a %s', classifier.__name__)
    model.fit(feature_vectors, candidate_pairs)
    LOGGER.info('Training done')
    return model


def _build(catalog, entity, dir_io):
    LOGGER.info("Building %s %s training set, I/O directory: '%s'",
                catalog, entity, dir_io)

    catalog_pid = target_database.get_pid(catalog)
    qids_and_tids = {}

    data_gathering.gather_target_ids(
        entity, catalog, catalog_pid, qids_and_tids)

    # Wikidata
    wd_io_path = os.path.join(dir_io, constants.WD_TRAINING % catalog)
    if os.path.exists(wd_io_path):
        LOGGER.info(
            "Will reuse existing Wikidata training set: '%s'", wd_io_path)
    else:
        LOGGER.info(
            "Building Wikidata training set, output file '%s' ...", wd_io_path)
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(qids_and_tids.keys(
            ), url_pids, ext_id_pids_to_urls, wd_io, qids_and_tids=qids_and_tids)

    wd_df_reader = read_json(wd_io_path, lines=True, chunksize=1000)

    LOGGER.info('Wikidata training set built')

    # Target
    target_io_path = os.path.join(
        dir_io, constants.TARGET_TRAINING % catalog)
    if os.path.exists(target_io_path):
        LOGGER.info("Will reuse existing %s training set: '%s'",
                    catalog, target_io_path)
    else:
        LOGGER.info(
            "Building target training set, output file '%s' ...", target_io_path)
        tids = set()
        for data in qids_and_tids.values():
            for identifier in data[constants.TID]:
                tids.add(identifier)
        # Dataset
        with gzip.open(target_io_path, 'wt') as target_io:
            data_gathering.gather_target_dataset(
                entity, catalog, tids, target_io, for_linking=False)

    # Enforce target id as a string
    target_df_reader = read_json(
        target_io_path, lines=True, chunksize=1000, dtype={constants.TID: str})

    LOGGER.info('Target training set built')

    return wd_df_reader, target_df_reader


def _block(wikidata_df, target_df):
    on_column = constants.TID

    LOGGER.info("Blocking on column '%s'", on_column)

    idx = rl.Index()
    idx.block(on_column)
    candidate_pairs = idx.index(wikidata_df, target_df)

    LOGGER.info('Blocking index built')

    return candidate_pairs


if __name__ == "__main__":
    pass
    # wd = read_json(
    #     '/tmp/soweego_shared/wikidata_discogs_training_set.json')
    # t = read_json('/tmp/soweego_shared/discogs_training_set')
    # workflow.clean(wd, t)
    # cp = block(wd, t)
    # fv = workflow.extract_features(cp, wd, t)
    # nb = rl.NaiveBayesClassifier(binarize=0.1)
    # nb.fit(fv, cp)
    # build('discogs', 'musician', '.')
    # tracemalloc.start()
    # execute('discogs', 'musician', '/Users/focs/soweego')
    # snapshot = tracemalloc.take_snapshot()
    # for stat in snapshot.statistics('lineno')[:10]:
    # print(stat)
