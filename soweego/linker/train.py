#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Training set construction for supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click
import recordlinkage as rl
from sklearn.externals import joblib

from soweego.commons import constants, target_database
from soweego.linker import workflow

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
    wd, target = workflow.preprocess('training', wd_reader, target_reader)
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

    # Wikidata
    wd_df_reader, qids_and_tids = workflow.build_wikidata(
        'training', catalog, entity, dir_io)

    # Target
    target_df_reader = workflow.build_target(
        'training', catalog, entity, qids_and_tids, dir_io)

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
    wid, t = _build('discogs', 'musician', '/Users/focs/soweego/output')
    print()
