# coding: utf-8
import csv
import json

import pandas
import recordlinkage

get_ipython().run_line_magic('cd', '~/discogs/')
from collections import defaultdict

r = csv.DictReader(open('discogs_sample_for_recordlinkage'), delimiter='\t')
discogs = defaultdict(list)
get_ipython().run_line_magic('cpaste', '')
discogs
discogs_df = pandas.DataFrame(discogs)
wikidata = defaultdict(list)
get_ipython().run_line_magic('cpaste', '')
wikidata
wikidata_df = pandas.DataFrame(wikidata)
wikidata_df
idx = recordlinkage.Index()
idx.block('name')
candidates = idx.index(discogs_df, wikidata_df)
len(candidates)
candidates
compare = recordlinkage.Compare()
compare.string('name', 'name', method='damerau_levenshtein', threshold=0.7)
get_ipython().run_line_magic('pinfo', 'compare.string')
vectors = compare.compute(candidates, discogs_df, wikidata_df)
compare = recordlinkage.Compare()
compare.string('name', 'name', method='levenshtein', threshold=0.7)
vectors = compare.compute(candidates, discogs_df, wikidata_df)
vectors
get_ipython().set_next_input('ecm = recordlinkage.ECM')
get_ipython().run_line_magic('pinfo', 'recordlinkage.ECM')
get_ipython().set_next_input('ecm = recordlinkage.ECMClassifier')
get_ipython().run_line_magic('pinfo', 'recordlinkage.ECMClassifier')
ecm = recordlinkage.ECMClassifier()
ecm.fit_predict(vectors)
vectors
get_ipython().set_next_input('nb = recordlinkage.NaiveBayesClassifier')
get_ipython().run_line_magic('pinfo', 'recordlinkage.NaiveBayesClassifier')
idx
type(idx)
type(candidates)
len(candidates)
candidates
idx.algorithms
len(candidates)
idx = recordlinkage.Index()
idx.full()
pairs = idx.index(discogs_df, wikidata_df)
len(pairs)
compare = recordlinkage.Compare()
compare.string('name', 'name', method='levenshtein', threshold=0.7)
feature_vectors = compare.compute(pairs, discogs_df, wikidata_df)
idx = recordlinkage.Index()
idx.block('name')
candidate_pairs = idx.index(discogs_df, wikidata_df)
len(candidate_pairs)
compare = recordlinkage.Compare()
compare.string('name', 'name', method='levenshtein', threshold=0.7)
features = compare.compute(candidate_pairs, discogs_df, wikidata_df)
features
compare = recordlinkage.Compare()
compare.string('name', 'name', method='levenshtein', threshold=0.7, label='stocazzo')
features = compare.compute(candidate_pairs, discogs_df, wikidata_df)
features
discogs_df[304]
discogs_df.catalog_id
features.describe()
features.sum(axis=1).value_counts().sort_index(ascending=False)
features[features.sum(axis=1) > 3]
features[features.sum(axis=1) > 3]
features.sum(axis=1)
features[features.sum(axis=1) > 1]
features[features.sum(axis=1) > 0]
from recordlinkage.datasets import load_febrl4

a, b = load_febrl4()
a
b
idx = recordlinkage.Index()
idx.block('given_name')
candidates = idx.index(a, b)
compare = recordlinkage.Compare()
get_ipython().run_line_magic('cpaste', '')
compare_cl = recordlinkage.Compare()
get_ipython().run_line_magic('cpaste', '')
features = compare.compute(candidates, a, b)
features = compare_cl.compute(candidates, a, b)
features
features.describe()
features.sum(axis=1).value_counts().sort_index(ascending=False)
features
features[features.sum(axis=1) > 3]
matches = features[features.sum(axis=1) > 3]
len(matches)
matches
get_ipython().run_line_magic('pinfo', 'pandas.Series')
from recordlinkage.preprocessing import clean

wikidata
etichette = json.load(open('/Users/focs/wikidata/label2qid_1_percent_sample.json'))
etichette
get_ipython().run_line_magic('pinfo', 'pandas.Series')
serie = pandas.Series(etichette)
serie
serie.axes
serie = pandas.Series(etichette.keys())
serie
serie = pandas.Series(list(etichette.keys()))
serie
clean(serie)
clean(serie, replace_by_none=None, strip_accents='unicode')
clean(serie, replace_by_none=None, strip_accents='ascii')
clean(serie, replace_by_none=None, strip_accents='unicode')
discogs
discogs_df
from recordlinkage.preprocessing import phonetic

get_ipython().run_line_magic('pinfo', 'phonetic')
serie[66]
giappa = pandas.Series([serie[66]])
giappa
phonetic(giappa, 'soundex')
phonetic(giappa, 'metaphone')
phonetic(giappa, 'nysiis')
phonetic(giappa, 'match_rating')
get_ipython().run_line_magic('hist', '')
cosa = pandas.Series(['àáâäæãåāèéêëēėęîïíīįìôöòóœøōõûüùúū'])
clean(cosa)
clean(cosa, replace_by_none=None)
clean(cosa, replace_by_none=None, strip_accents='ascii')
clean(cosa, replace_by_none=None, strip_accents='unicode')
etichette
# names = []
'''
хартшорн, чарльзcharles hartshorne,
 'チャールズ・ハートショーン': 'Q1064777',
 'تشارلز هارتشورن': 'Q1064777',
 'چارلز هارتسهورن': 'Q1064777',
 '찰스 하츠혼': 'Q1064777',
'''
names = [
    "хартшорн, чарльз",
    "charles hartshorne",
    'チャールズ・ハートショーン',
    'تشارلز هارتشورن',
    '찰스 하츠혼',
]
names
clean(names)
names = pandas.Series(
    [
        "хартшорн, чарльз",
        "charles hartshorne",
        'チャールズ・ハートショーン',
        'تشارلز هارتشورن',
        '찰스 하츠혼',
    ]
)
clean(names)
clean(names, replace_by_none=None, strip_accents='ascii')
names = pandas.Series(
    [
        "хартшорн, чарльз",
        "charles hartshorne",
        'チャールズ・ハートショーン',
        'تشارلز هارتشورن',
        '찰스 하츠혼',
        'àáâäæãåāèéêëēėęîïíīįìôöòóœøōõûüùúū',
    ]
)
clean(names)
clean(names, replace_by_none=None, strip_accents='ascii')
