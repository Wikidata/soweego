#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Implementation of custom classifiers for use with Record Linkage."""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, tupini07'


import pandas as pd
from recordlinkage.adapters import SKLearnAdapter
from recordlinkage.base import BaseClassifier
from sklearn.svm import SVC


class SVCClassifier(SKLearnAdapter, BaseClassifier):
    def __init__(self, *args, probability=True, **kwargs):
        super(SVCClassifier, self).__init__()

        kwargs['probability'] = probability

        self.kernel = SVC(*args, **kwargs)

    def prob(self, x):

        # The `predict_proba` method of the SVC classifier
        # returns a matrix where each row corresponds to
        # one element, and the columns correspont to the classes
        # the classifier was trained on. In our case it was
        # trained only on 2 [0,1]. Since we only want the
        # probability that the elements match then we return
        # only the second column, which is the probability
        # of belonging to the class 1

        assert self.kernel.classes_[1] == 1, ('Invalid classes, the SVC predict probability '
                                              'expects that the second class in the trained '
                                              f'model is 1. It currently is: {self.kernel.classes_[1]}')

        probabilities = self.kernel.predict_proba(x)[:, 1]

        return pd.DataFrame(probabilities, index=x.index)

    def __repr__(self):
        return f'{self.kernel}'
