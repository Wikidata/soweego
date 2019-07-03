# Notes on the recordlinkage Python library
https://recordlinkage.readthedocs.io/

## General
- uses `pandas` for data structures, typically the `DataFrame`, `Series`, and `MultiIndex` classes;
- https://pandas.pydata.org/pandas-docs/stable/dsintro.html;
- https://pandas.pydata.org/pandas-docs/stable/advanced.html;
- uses `jellyfish` under the hood for edit distances and phonetic algorithms.

## Data format
- https://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe;
- https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html;
- uses `pandas.DataFrame` to represent datasets. It's basically a table with column headers;
- conversion from a `dict` is easy: key = column header, value = cell;
- a value is a list, so `defaultdict(list)` is helpful;
```py
dataset = pandas.DataFrame(
  {
    'catalog_id': [666, 777, 888],
    'name': ['huey', 'dewey', 'louie'],
    ...
  }
)
```
- remember the order of values, i.e., `666` -> `'huey'`;

## Cleaning
- AKA **pre-processing** AKA **normalization** AKA **standardization**;
- https://recordlinkage.readthedocs.io/en/latest/ref-preprocessing.html;
- uses `pandas.Series`, a list-like object;
- the `clean` function seems interesting at a first glimpse;
- by default, it **removes text inside brackets**. Might be useful, trivial to re-implement;
- terrible default regex, **removes everything that is not an ASCII letter!** Non-ASCII strings are just deleted! Use a custom regex or `None` in `replace_by_none=` kwarg to avoid this;
- nice ASCII folding via `strip_accents='ascii'`, **not done** by default;
- `strip_accents='unicode'` keeps intact some Unicode chars, e.g., `œ`;
- non-latin scripts are just not handled;
- the `phonetic` function has the same problems as in `jellyfish`, see [#79](https://github.com/Wikidata/soweego/issues/79).
```py
from recordlinkage.preprocessing import clean

names = pandas.Series(
  [
    'хартшорн, чарльз',
    'charles hartshorne',
    'チャールズ・ハートショーン',
    'تشارلز هارتشورن',
    '찰스 하츠혼',
    àáâäæãåāèéêëēėęîïíīįìôöòóœøōõûüùúū'
  ]
)
```
```py
clean(names)
```
Output:
```
0
1    charles hartshorne
2
3
4
5
dtype: object
```
```py
clean(names, replace_by_none=None, strip_accents='ascii')
```
Output:
```
0                                  ,
1                 charles hartshorne
2
3
4
5    aaaaaaaeeeeeeeiiiiiioooooouuuuu
dtype: object
```

## Indexing
- AKA **blocking** AKA **candidate acquisition**;
- https://recordlinkage.readthedocs.io/en/latest/ref-index.html;
- make pairs of records to reduce the space complexity (quadratic);
- a simple call to the `Index.block(FIELD)` function is not enough for names, as it makes pairs that **exactly** agree, i.e., **like an exact match**;
```py
index = recordlinkage.Index()
index.block('name')
candidate_pairs = index.index(source_dataset, target_dataset)
```
- we could inject the MariaDB full-text index [#126](https://github.com/Wikidata/soweego/issues/126) as a **user-defined algorithm**;
- https://recordlinkage.readthedocs.io/en/latest/ref-index.html#user-defined-algorithms;
- https://recordlinkage.readthedocs.io/en/latest/ref-index.html#examples.

## Comparing
- https://recordlinkage.readthedocs.io/en/latest/ref-compare.html;
- can be seen as **feature extraction**;
- probably useful for [#143](https://github.com/Wikidata/soweego/issues/143);
- the `Compare.date` function can be useful for dates: https://recordlinkage.readthedocs.io/en/latest/ref-compare.html#recordlinkage.compare.Date;
- the `Compare.string` function implements `jellyfish` string edit distances + others: https://recordlinkage.readthedocs.io/en/latest/ref-compare.html#recordlinkage.compare.String;
- the string **edit distance feature** is **binary**, not **scalar**: `feature_vectors.sum(1).value_counts()` below shows that;
- the `threshold` kwarg gives a binary score for pairs above or below its value, i.e., `1` or `0`. **It's not really a threshold**;
- not clear how the feature is fired by default, i.e., `threshold=None`;
- better always use the `threshold` kwarg then, typically `3` for Levenshtein and `0.85` for Jaro-Winkler;
```py
comp = recordlinkage.Compare()
comp.string('name', 'label', threshold=3)
feature_vectors = comp.compute(candidate_pairs, source_dataset, target_dataset)
print(feature_vectors.sum(1).value_counts())
```

## Classification
- train with `fit(training_feature_vectors, match_index)`;
- classify with `predict(classification_feature_vectors)`;
- we could give SVM a try: https://recordlinkage.readthedocs.io/en/latest/notebooks/classifiers.html#Support-Vector-Machines;
- very recent docs update (Dec 17 2018!) on adapters: https://github.com/J535D165/recordlinkage/blob/master/docs/ref-classifiers.rst#adapters;
- **it seems possible to inject a neural network with `keras`**;
- remember to set comparison of fields with missing values to `0`, i.e., pair disagreement:
  - _Most classifiers can not handle comparison vectors with missing values._;
  - no worries, `compare.string` does that by default.

## Training workflow
INPUT = training set = existing QIDs with target IDs = dict `{ QID: target_ID }`;
1. get the QID statements from Wikidata;
2. query MariaDB for target ID data;
3. load both into 2 `pandas.DataFrame`;
4. pre-process;
5. make the index with blocking -> `match_index` arg;
6. feature extraction with comparison -> `training_feature_vectors` arg.

## Naïve Bayes
- https://recordlinkage.readthedocs.io/en/latest/ref-classifiers.html#recordlinkage.NaiveBayesClassifier;
- https://recordlinkage.readthedocs.io/en/latest/notebooks/classifiers.html;
- **Code example**: https://github.com/J535D165/recordlinkage/blob/master/examples/supervised_learning_prob.py
- `recordlinkage.NaiveBayesClassifier` class;
- works with **binary features**, also explains why the edit distance feature is binary;
- **not sure** what the `binarize` argument means. The docs say: _Threshold for binarizing (mapping to booleans) of sample features. If None, input is presumed to consist of multilevel vectors_;
- the code example uses `binary_vectors` and sets toy `m` and `u` probabilities:
  1. are comparison vectors (point 6 of the training workflow) the expected input?
  2. should we compute `m` and `u` on our own as well?

