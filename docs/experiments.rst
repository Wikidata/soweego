Experiments
===========


Default evaluation technique
----------------------------

Applies to all experiments:

-  stratified 5-fold cross validation over training/test splits;
-  mean performance scores over the folds.

Single-layer perceptron optimizers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`https://github.com/Wikidata/soweego/issues/285 <https://github.com/Wikidata/soweego/issues/285>`__

Setting
~~~~~~~

-  run: May 3 2019;
-  output folder: ``soweego-2.eqiad.wmflabs:/srv/dev/20190503/``;
-  head commit: d0d390e622f2782a49a1bd0ebfc64478ed34aa0c;
-  command:
   ``python -m soweego linker evaluate slp ${Dataset} ${Entity} optimizer=${Optimizer}``.

Discogs band
~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .782      .945   .856
rmsprop   .801      .930   .860
nadam     .805      .925   .861
adamax    .795      .938   .861
adam      .800      .929   .860
adagrad   .802      .927   .859
adadelta  .799      .934   .861
========= ========= ====== =======

Discogs musician
~~~~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .815      .985   .892
rmsprop   .816      .985   .893
nadam     .816      .986   .893
adamax    .817      .985   .893
adam      .816      .985   .893
adagrad   .816      .986   .893
adadelta  .815      .986   .892
========= ========= ====== =======

IMDb director
~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .918      .954   .936
rmsprop   .895      .954   .923
nadam     .908      .954   .930
adamax    .907      .955   .930
adam      .909      .953   .931
adagrad   .867      .950   .907
adadelta  .902      .954   .927
========= ========= ====== =======

IMDb musician
~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .912      .927   .920
rmsprop   .913      .929   .921
nadam     .913      .929   .921
adamax    .913      .928   .921
adam      .913      .928   .921
adagrad   .873      .860   .866
adadelta  .913      .928   .921
========= ========= ====== =======

IMDb producer
~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .917      .942   .929
rmsprop   .916      .938   .927
nadam     .916      .938   .927
adamax    .916      .940   .928
adam      .916      .938   .927
adagrad   .852      .684   .756
adadelta  .916      .939   .928
========= ========= ====== =======

IMDb writer
~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .929      .943   .936
rmsprop   .927      .940   .934
nadam     .930      .940   .935
adamax    .930      .941   .935
adam      .930      .940   .935
adagrad   .872      .923   .896
adadelta  .931      .941   .936
========= ========= ====== =======

MusicBrainz band
~~~~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .952      .869   .909
rmsprop   .949      .875   .911
nadam     .949      .877   .911
adamax    .952      .871   .910
adam      .951      .875   .911
adagrad   .932      .886   .909
adadelta  .952      .874   .911
========= ========= ====== =======

MusicBrainz musician
~~~~~~~~~~~~~~~~~~~~

========= ========= ====== =======
Optimizer Precision Recall F-score
========= ========= ====== =======
sgd       .942      .957   .949
rmsprop   .941      .958   .949
nadam     .941      .958   .949
adamax    .941      .958   .949
adam      .941      .958   .949
adagrad   .946      .953   .950
adadelta  .941      .958   .950
========= ========= ====== =======

Takeaways
~~~~~~~~~

-  All optimizers seem to do a similar job;
-  no specific impact on the performance.

Max Levenshtein VS average Levenshtein
--------------------------------------

`https://github.com/Wikidata/soweego/issues/176 <https://github.com/Wikidata/soweego/issues/176>`__



Setting
~~~~~~~

-  run: May 7 2019;
-  output folder: ``soweego-2.eqiad.wmflabs:/srv/dev/20190507/``;
-  head commit: ddd5d719793ea217267413a52d1d2e5b90c341a7;
-  command:
   ``python -m soweego linker evaluate ${Algorithm} ${Dataset} ${Entity}``.



Discogs band
~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .787      .955   **.863**
nb avg    .789      .941   .859
lsvm max  .780      .960   **.861**
lsvm avg  .785      .946   .858
svm max   .777      .963   .860
svm avg   .777      .963   .860
slp max   .784      .954   **.861**
slp avg   .776      .956   .857
mlp max   .822      .925   .870
========= ========= ====== ========



Discogs musician
~~~~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .831      .975   **.897**
nb avg    .836      .958   .893
lsvm max  .818      .985   **.894**
lsvm avg  .814      .986   .892
svm max   .815      .985   .892
svm avg   .815      .985   .892
slp max   .821      .983   **.895**
slp avg   .815      .985   .892
mlp max   .852      .963   .904
========= ========= ====== ========



IMDb director
~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .896      .971   .932
nb avg    .897      .971   .932
lsvm max  .919      .943   **.931**
lsvm avg  .919      .942   .930
svm max   .911      .950   .930
svm avg   .908      .958   **.932**
slp max   .917      .953   **.935**
slp avg   .867      .953   .908
mlp max   .913      .964   .938
========= ========= ====== ========



IMDb musician
~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .889      .962   .924
nb avg    .891      .960   .924
lsvm max  .917      .938   .927
lsvm avg  .917      .937   .927
svm max   .904      .944   .924
svm avg   .908      .942   .924
slp max   .924      .929   **.926**
slp avg   .922      .914   .918
mlp max   .912      .951   .931
========= ========= ====== ========



IMDb producer
~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .870      .971   .918
nb avg    .871      .970   .918
lsvm max  .920      .940   **.930**
lsvm avg  .920      .938   .929
svm max   .923      .927   .925
svm avg   .923      .926   .925
slp max   .914      .940   **.927**
slp avg   .862      .914   .883
mlp max   .911      .956   .933
========= ========= ====== ========



IMDb writer
~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .904      .975   **.938**
nb avg    .910      .961   .935
lsvm max  .936      .949   **.943**
lsvm avg  .936      .948   .942
svm max   .932      .954   .943
svm avg   .932      .954   .943
slp max   .938      .946   **.942**
slp avg   .903      .955   .928
mlp max   .930      .963   .946
========= ========= ====== ========



MusicBrainz band
~~~~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb max    .821      .987   .896
nb avg    .822      .985   .896
lsvm max  .944      .879   .910
lsvm avg  .943      .888   **.914**
svm max   .930      .891   .910
svm avg   .939      .893   **.915**
slp max   .953      .865   .907
slp avg   .930      .885   .907
mlp max   .906      .918   .911
========= ========= ====== ========



MusicBrainz musician
~~~~~~~~~~~~~~~~~~~~

========= ========= ====== =======
Algorithm Precision Recall F-score
========= ========= ====== =======
nb max    .955      .936   .946
nb avg    .955      .936   .946
lsvm max  .941      .963   .952
lsvm avg  .941      .962   .952
svm max   .951      .938   .944
svm avg   .950      .938   .944
slp max   .942      .957   .949
slp avg   .943      .956   .949
mlp max   .939      .970   .954
========= ========= ====== =======



Takeaways
~~~~~~~~~

Max Levenshtein has the following impact:

-  NB is always improved or left untouched;
-  LSVM is always improved, left untouched for IMDb director, but
   worsens for MusicBrainz band;
-  SVM is often left untouched, but worsens for IMDb director and
   MusicBrainz band;
-  SLP is always improved with the highest impact, left untouched for
   MusicBrainz;
-  **conclusion:** max Levenshtein should replace the average one.

String kernel feature
---------------------

`https://github.com/Wikidata/soweego/issues/174 <https://github.com/Wikidata/soweego/issues/174>`__



Setting
~~~~~~~

-  run: May 8 2019;
-  output folder: ``soweego-2.eqiad.wmflabs:/srv/dev/20190508/``;
-  head commit: 0c5137fc4fe446abdb6df6dbde277b7aa15881c5;
-  command:
   ``python -m soweego linker evaluate ${Algorithm} ${Dataset} ${Entity}``.



Discogs band
~~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .788      **.942** .859
nb        .789      .941     .859
lsvm +sk  .785      .946     .858
lsvm      .785      .946     .858
svm +sk   .778      .963     **.861**
svm       .777      .963     .860
slp +sk   **.783**  .947     .857
slp       .776      **.956** .857
mlp +sk   .848      .913     .879
========= ========= ======== ========



Discogs musician
~~~~~~~~~~~~~~~~

========= ========= ======== =======
Algorithm Precision Recall   F-score
========= ========= ======== =======
nb +sk    .836      .958     .893
nb        .836      .958     .893
lsvm +sk  **.816**  .985     .892
lsvm      .814      **.986** .892
svm +sk   .815      .985     .892
svm       .815      .985     .892
slp +sk   **.820**  .978     .892
slp       .815      **.985** .892
mlp +sk   .868      .948     .906
========= ========= ======== =======



IMDb director
~~~~~~~~~~~~~

========= ========= ====== ========
Algorithm Precision Recall F-score
========= ========= ====== ========
nb +sk    .897      .971   .932
nb        .897      .971   .932
lsvm +sk  .923      .949   **.935**
lsvm      .919      .942   .930
svm +sk   **.914**  .950   .931
svm       .908      .958   **.932**
slp +sk   **.918**  .955   **.936**
slp       .867      .953   .908
mlp +sk   .918      .964   .941
========= ========= ====== ========



IMDb musician
~~~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .891      **.961** .924
nb        .891      .960     .924
lsvm +sk  .922      .941     **.931**
lsvm      .917      .937     .927
svm +sk   .910      .949     **.929**
svm       .908      .942     .924
slp +sk   .922      .934     **.928**
slp       .922      .914     .918
mlp +sk   .914      .958     .935
========= ========= ======== ========



IMDb producer
~~~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .871      .970     .918
nb        .871      .970     .918
lsvm +sk  .921      .943     **.932**
lsvm      .920      .938     .929
svm +sk   .923      **.927** .925
svm       .923      .926     .925
slp +sk   .916      .942     **.929**
slp       .862      .914     .883
mlp +sk   .912      .959     .935
========= ========= ======== ========



IMDb writer
~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .910      .961     .935
nb        .910      .961     .935
lsvm +sk  .938      .953     **.945**
lsvm      .936      .948     .942
svm +sk   .933      .957     **.945**
svm       .932      .954     .943
slp +sk   .939      .948     **.943**
slp       .903      **.955** .928
mlp +sk   .931      .968     .949
========= ========= ======== ========



MusicBrainz band
~~~~~~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .821      .985     .896
nb        **.822**  .985     .896
lsvm +sk  .940      .895     **.917**
lsvm      **.943**  .888     .914
svm +sk   .937      .899     **.918**
svm       **.939**  .893     .915
slp +sk   .952      .873     **.911**
slp       .930      **.885** .907
mlp +sk   .937      .904     .920
========= ========= ======== ========



MusicBrainz musician
~~~~~~~~~~~~~~~~~~~~

========= ========= ======== ========
Algorithm Precision Recall   F-score
========= ========= ======== ========
nb +sk    .955      .936     .946
nb        .955      .936     .946
lsvm +sk  .938      **.965** .951
lsvm      .941      .962     **.952**
svm +sk   **.951**  .938     .944
svm       .950      .938     .944
slp +sk   .941      .958     **.950**
slp       **.943**  .956     .949
mlp +sk   .939      .972     .955
========= ========= ======== ========



Takeaways
~~~~~~~~~

The string kernel feature:

-  has the most positive impact on SLP;
-  slightly improves performance in most cases, but sligthly worsens:

   -  precision in 1 case, i.e., NB for MusicBrainz band;
   -  recall in 3 cases, i.e., SLP for Discogs band, LSVM & SLP for
      Discogs musician;
   -  f-score in 2 cases, i.e., SVM for IMDb director, LSVM for
      MusicBrainz musician.

-  **conclusion**: the string kernel feature should be added.
