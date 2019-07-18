Evaluations
===========

Setting
-------

-  run: April 11 2019 on ``soweego-1`` VPS instance;
-  output folder: ``/srv/dev/20190411``;
-  head commit: 1505429997b878568a9e24185dc3afa7ad4720eb;
-  command:
   ``python -m soweego linker evaluate ${Algorithm} ${Dataset} ${Entity}``;
-  evaluation technique: stratified 5-fold cross validation over
   training/test splits;
-  mean performance scores over the folds.

Algorithms parameters
---------------------

-  Naïve Bayes (NB):

   -  binarize = 0.1;
   -  alpha = 0.0001;

-  ``liblinear`` SVM (LSVM): default parameters as per scikit
   `LinearSVC <https://scikit-learn.org/stable/modules/generated/sklearn.svm.LinearSVC.html>`__;
-  ``libsvm`` SVM (SVM):

   -  kernel = linear;
   -  other parameters as per scikit
      `SVC <https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html>`__
      defaults;

-  single-layer perceptron (SLP):

   -  layer = fully connected (``Dense``);
   -  activation = sigmoid;
   -  optimizer = stochastic gradient descent;
   -  loss = binary cross-entropy;
   -  training batch size = 1,024;
   -  training epochs = 100.

-  multi-layer perceptron (MLP):

   -  layers = 128 > BN > 32 > BN > 1

      -  fully connected layers followed by BatchNormalization (BN)

   -  activation:

      -  hidden layers = relu;
      -  output layer = sigmoid;

   -  optimizer = Adadelta;
   -  loss = binary cross-entropy
   -  training batch size = 1,024;
   -  training epochs = 1000;
   -  early stopping:

      -  patience = 100;

Performance
-----------

========= =========== ======== =============== ============ =============
Algorithm Dataset     Entity   Precision (std) Recall (std) F-score (std)
========= =========== ======== =============== ============ =============
NB        Discogs     Band     .789 (.0031)    .941 (.0004) .859 (.002)
LSVM      Discogs     Band     .785 (.0058)    .946 (.0029) .858 (.0034)
SVM       Discogs     Band     .777 (.003)     .963 (.0016) .86 (.0024)
SLP       Discogs     Band     .776 (.0041)    .956 (.0012) .857 (.0029)
NB        Discogs     Musician .836 (.0018)    .958 (.0012) .893 (.0013)
SVM       Discogs     Musician .814 (.0015)    .986 (.0003) .892 (.001)
SLP       Discogs     Musician .815 (.002)     .985 (.0006) .892 (.0012)
NB        IMDb        Actor    TODO            TODO         TODO
SVM       IMDb        Actor    TODO            TODO         TODO
SLP       IMDb        Actor    TODO            TODO         TODO
MLP       IMDb        Actor    TODO            TODO         TODO
NB        IMDb        Director .897 (.00195)   .971 (.0012) .932 (.001)
SVM       IMDb        Director .919 (.0031)    .942 (.0019) .93 (.002)
SLP       IMDb        Director .867 (.0115)    .953 (.0043) .908 (.0056)
NB        IMDb        Musician .891 (.0042)    .96 (.0022)  .924 (.0026)
SVM       IMDb        Musician .917 (.0043)    .937 (.0034) .927 (.003)
SLP       IMDb        Musician .922 (.005)     .914 (.0092) .918 (.0055)
NB        IMDb        Producer .871 (.0023)    .97 (.0037)  .918 (.0011)
SVM       IMDb        Producer .92 (.005)      .938 (.0038) .929 (.0026)
SLP       IMDb        Producer .862 (.0609)    .914 (.0648) .883 (.0185)
NB        IMDb        Writer   .91 (.003)      .961 (.0022) .935 (.0022)
SVM       IMDb        Writer   .936 (.0029)    .948 (.0025) .942 (.0026)
SLP       IMDb        Writer   .903 (.0154)    .955 (.0147) .928 (.0047)
NB        MusicBrainz Band     .822 (.00169)   .985 (.0008) .896 (.001)
SVM       MusicBrainz Band     .943 (.0019)    .888 (.0027) .914 (.0016)
SLP       MusicBrainz Band     .93 (.0265)     .885 (.0103) .907 (.0082)
NB        MusicBrainz Musician .955 (.0009)    .936 (.0011) .946 (.00068)
SVM       MusicBrainz Musician .941 (.0011)    .962 (.001)  .952 (.0004)
SLP       MusicBrainz Musician .943 (.0018)    .956 (.0019) .949 (.0007)
========= =========== ======== =============== ============ =============

Confidence
----------

The following plots display the confidence scores distribution and the
total predictions yielded by each algorithm on each target
classification set.

Note that linear SVM is omitted since it does not output probability
scores.

Axes:

-  x = # predictions;
-  y = confidence score.

Discogs band
~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108108/discogs_band_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108107/discogs_band_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108106/discogs_band_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161806/discogs_band_mlp.pdf>`__

Discogs musician
~~~~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108104/discogs_musician_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108101/discogs_musician_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108102/discogs_musician_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161809/discogs_musician_mlp.pdf>`__

IMDb director
~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108118/imdb_director_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108119/imdb_director_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108117/imdb_director_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161811/imdb_director_mlp.pdf>`__

IMDb musician
~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108109/imdb_musician_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108110/imdb_musician_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108112/imdb_musician_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161813/imdb_musician_mlp.pdf>`__

IMDb producer
~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108123/imdb_producer_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108121/imdb_producer_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108122/imdb_producer_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161815/imdb_producer_mlp.pdf>`__

IMDb writer
~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108115/imdb_writer_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108113/imdb_writer_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108114/imdb_writer_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161819/imdb_writer_mlp.pdf>`__

MusicBrainz band
~~~~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108129/musicbrainz_band_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108130/musicbrainz_band_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108132/musicbrainz_band_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161832/musicbrainz_band_mlp.pdf>`__

MusicBrainz musician
~~~~~~~~~~~~~~~~~~~~

`NB <https://github.com/Wikidata/soweego/files/3108125/musicbrainz_musician_nb_linker_result.pdf>`__,
`SVM <https://github.com/Wikidata/soweego/files/3108127/musicbrainz_musician_svm_linker_result.pdf>`__,
`SLP <https://github.com/Wikidata/soweego/files/3108128/musicbrainz_musician_slp_linker_result.pdf>`__.
`MLP <https://github.com/Wikidata/soweego/files/3161833/musicbrainz_musician_mlp.pdf>`__

Comparison
----------

See the plots above to have a rough idea on the amount of confident
predictions.

Threshold values:

-  # predictions >= 0.0000000001, i.e., equivalent to almost all
   matches;
-  # confident >= 0.8.



Discogs band
~~~~~~~~~~~~

WD items: 50,316

============= ==== ==== ====== ====== ======
Measure       NB   LSVM SVM    SLP    MLP
============= ==== ==== ====== ====== ======
Precision     .789 .785 .777   .776   .833
Recall        .941 .946 .963   .957   .914
F-score       .859 .858 .86    .857   .872
# predictions 820  51   94,430 91,295 91,132
# confident   219  N.A. 1,660  5,355  11,114
============= ==== ==== ====== ====== ======



Discogs musician
~~~~~~~~~~~~~~~~

WD items: 199,180

============= ===== ==== ======= ======= =======
Measure       NB    LSVM SVM     SLP     MLP
============= ===== ==== ======= ======= =======
Precision     .836  .814 .815    .815    .849
Recall        .958  .986 .985    .985    .961
F-score       .893  .892 .892    .892    .902
# predictions 3,872 200  533,301 517,450 514,488
# confident   1,101 N.A. 98,172  58,437  57,184
============= ===== ==== ======= ======= =======



IMDb director
~~~~~~~~~~~~~

WD items: 9,249

============= ==== ==== ====== ====== ======
Measure       NB   LSVM SVM    SLP    MLP
============= ==== ==== ====== ====== ======
Precision     .897 .919 .908   .867   .916
Recall        .971 .942 .958   .953   .961
F-score       .932 .93  .932   .908   .938
# predictions 192  10   17,557 17,187 16,881
# confident   60   N.A. 1,616  553    1,810
============= ==== ==== ====== ====== ======



IMDb musician
~~~~~~~~~~~~~

WD items: 217,139

============= ===== ==== ======= ======= =======
Measure       NB    LSVM SVM     SLP     MLP
============= ===== ==== ======= ======= =======
Precision     .891  .917 .908    .922    .903
Recall        .96   .937 .942    .914    .951
F-score       .924  .927 .924    .918    .926
# predictions 4,806 218  406,674 398,346 376,857
# confident   1,341 N.A. 21,462  7,244   16,272
============= ===== ==== ======= ======= =======



IMDb producer
~~~~~~~~~~~~~

WD items: 2,251

============= ==== ==== ===== ===== =====
Measure       NB   LSVM SVM   SLP   MLP
============= ==== ==== ===== ===== =====
Precision     .871 .92  .923  .862  .912
Recall        .97  .938 .926  .914  .956
F-score       .918 .929 .925  .883  .933
# predictions 56   3    5,249 5,116 5,094
# confident   15   N.A. 507   180   529
============= ==== ==== ===== ===== =====



IMDb writer
~~~~~~~~~~~

WD items: 16,446

============= ==== ==== ====== ====== ======
Measure       NB   LSVM SVM    SLP    MLP
============= ==== ==== ====== ====== ======
Precision     .91  .936 .932   .903   .921
Recall        .961 .948 .954   .955   .962
F-score       .935 .942 .943   .928   .941
# predictions 428  17   45,122 44,338 43,868
# confident   138  N.A. 2,934  1,548  3,234
============= ==== ==== ====== ====== ======



MusicBrainz band
~~~~~~~~~~~~~~~~

WD items: 32,658

============= ==== ==== ====== ====== ======
Measure       NB   LSVM SVM    SLP    MLP
============= ==== ==== ====== ====== ======
Precision     .822 .943 .939   .93    .933
Recall        .985 .888 .893   .885   .902
F-score       .896 .914 .915   .907   .918
# predictions 265  33   39,618 38,012 33,981
# confident   46   N.A. 1,475  501    1,506
============= ==== ==== ====== ====== ======



MusicBrainz musician
~~~~~~~~~~~~~~~~~~~~

WD items: 153,725

============= ===== ==== ======= ======= =======
Measure       NB    LSVM SVM     SLP     MLP
============= ===== ==== ======= ======= =======
Precision     .955  .941 .95     .943    .940
Recall        .936  .962 .938    .956    .968
F-score       .946  .952 .944    .949    .954
# predictions 2,833 154  280,029 260,530 194,505
# confident   1,212 N.A. 7,496   7,339   8,470
============= ===== ==== ======= ======= =======
