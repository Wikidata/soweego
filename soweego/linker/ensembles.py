import logging
from typing import List, Tuple, Union

import pandas as pd
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)


def remove_duplicates_by_majority_vote(df: pd.DataFrame, threshold=0.0) -> pd.DataFrame:
    """
    Takes a dataframe which represents the predictions given by multiple classifiers.
    These predictions will most certainly have duplicate values (predictions for the
    same entity pair given by different classifiers).

    This method checks the predictions yielded for all entities, and if, for one entity,
    the majority of predictions is above the provided threshold then the prediction is left
    in the final set, otherwise it is removed.

    :param df: The dataframe containing the predictions
    :param threshold: Predictions with certainty above this threshold are considered as matches

    :return: A dataframe containing the predictions filtered by majority vote
    """

    # Ensure that index is sorted
    df = df.sort_index()

    # where we will save the result
    # remove duplicates so that we go through them only once
    result = df.copy(deep=True)[~df.index.duplicated()]

    # at the end we will drop these rows
    rows_to_drop = []

    LOGGER.info('Joining classifier results via "majority voting", using threshold "%s" ', threshold)

    def is_majority_voted(idx: Tuple[str, str]) -> Tuple[bool, float]:
        """Tells us if the majority of predictions say that the `idx` should stay"""

        preds = df.loc[idx]
        positive_votes = (preds >= threshold).sum()
        perc_votes = float(positive_votes / len(preds))

        # if majority of votes say it should stay then we return
        # the maximum prediction. Else return None, so that the
        # idx is removed from the final dataframe
        if perc_votes >= 0.5:
            return True, float(preds.max())
        else:
            LOGGER.debug('Entry with index "%s" will be dropped since only has "%s" of votes ..', idx, perc_votes)
            return False, float(preds.min())

    for idx, _ in tqdm(result.iterrows(), total=len(result)):
        is_good, pred = is_majority_voted(idx)

        if is_good:
            result.loc[idx] = pred
        else:
            rows_to_drop.append(idx)

    result.drop(index=rows_to_drop, inplace=True)

    return result


def join_dataframes_by_union(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Joins dataframes via set "union"
    """
    return pd.concat(dfs, join='outer')


def join_dataframes_by_intersection(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Joins dataframes via set "intersection"
    """
    return pd.concat(dfs, join='inner')
