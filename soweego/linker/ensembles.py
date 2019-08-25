import logging
from typing import List, Tuple, Union

import pandas as pd
from tqdm import tqdm

from soweego.commons import constants

LOGGER = logging.getLogger(__name__)


def remove_duplicates_by_majority_vote(
        df: pd.Series, threshold=0.0
) -> pd.Series:
    """
    Takes a pd.Series which represents the predictions given by multiple classifiers.
    These predictions will most certainly have duplicate values (predictions for the
    same entity pair given by different classifiers).

    This method checks the predictions yielded for all entities, and if, for one entity,
    the majority of predictions is above the provided threshold then the prediction is left
    in the final set, otherwise it is removed.

    :param df: The pd.Series containing the predictions
    :param threshold: Predictions with certainty above this threshold are considered as matches

    :return: A pd.Series containing the predictions filtered by majority vote
    """

    # Ensure that index is sorted
    df = df.sort_index()

    # where we will save the result
    # remove duplicates so that we go through them only once
    result = df.copy(deep=True)[~df.index.duplicated()]

    # at the end we will drop these rows
    rows_to_drop = []

    LOGGER.info(
        'Joining classifier results via "majority voting", using threshold "%s" ',
        threshold,
    )

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
            LOGGER.debug(
                'Entry with index "%s" will be dropped since only has "%s" of votes ..',
                idx,
                perc_votes,
            )
            return False, float(preds.min())

    for idx, _ in tqdm(result.iteritems(), total=len(result)):
        is_good, pred = is_majority_voted(idx)

        if is_good:
            result.loc[idx] = pred
        else:
            rows_to_drop.append(idx)

    result.drop(index=rows_to_drop, inplace=True)

    return result


def remove_duplicates_by_averaging(
        df: pd.Series, threshold=0.0
) -> pd.Series:
    """
    Takes a pd.Series which represents the predictions given by multiple classifiers.
    It will average all duplicate predictions.

    :param df: The Series containing the predictions
    :param threshold: Predictions with certainty above this threshold are considered as matches

    :return: A Series containing the averaged predictions
    """

    # Ensure that index is sorted
    df = df.sort_index()

    # where we will save the result
    # remove duplicates so that we go through them only once
    result = df.copy(deep=True)[~df.index.duplicated()]

    # at the end we will drop these rows
    rows_to_drop = []

    LOGGER.info(
        'Joining classifier results via "averaging", using threshold "%s" ',
        threshold,
    )

    def get_average_for_idx(idx: str) -> Union[float, bool]:

        preds = df.loc[idx]
        avg = float(preds.mean())

        return avg

    for idx, _ in tqdm(result.iteritems(), total=len(result)):
        avg = get_average_for_idx(idx)

        if avg >= threshold:
            result.loc[idx] = avg
        else:
            LOGGER.debug(
                'Entry with index "%s" will be dropped. It has an average of "%s"',
                idx,
                avg,
            )
            rows_to_drop.append(idx)

    result.drop(index=rows_to_drop, inplace=True)

    return result


def join_predictions_by_union(dfs: List[pd.Series]) -> pd.Series:
    """
    Joins dataframes via set "union"
    """
    return pd.concat(dfs, join='outer')


def join_predictions_by_intersection(dfs: List[pd.Series]) -> pd.Series:
    """
    Joins dataframes via set "intersection"
    """
    return pd.concat(dfs, join='inner')


def assert_join_merge_keywords(how_to_join: str, how_to_rem_duplicates: str):
    assert how_to_join in constants.SC_AVAILABLE_JOIN, (
            'The provided join method needs to be one of: '
            + str(constants.SC_AVAILABLE_JOIN)
    )

    assert how_to_rem_duplicates in constants.SC_AVAILABLE_COMBINE, (
            'The provided combine method needs to be one of: '
            + str(constants.SC_AVAILABLE_COMBINE)
    )


def ensemble_predictions_by_keywords(all_results: List[pd.Series],
                                     threshold: float,
                                     how_to_join: str,
                                     how_to_rem_duplicates: str) -> pd.DataFrame:

    for k in all_results:
        assert isinstance(k, pd.Series), 'All predictions should be pd.Series'

    merged_results: pd.DataFrame
    if how_to_join == constants.SC_UNION:
        merged_results = join_predictions_by_union(all_results)

    elif how_to_join == constants.SC_INTERSECTION:
        merged_results = join_predictions_by_intersection(all_results)

    # and then proceed to deal with duplicates. This step also removes entries under the
    # specified threshold
    if how_to_rem_duplicates == constants.SC_AVERAGE:
        merged_results = remove_duplicates_by_averaging(
            merged_results, threshold
        )

    elif how_to_rem_duplicates == constants.SC_VOTING:
        merged_results = remove_duplicates_by_majority_vote(
            merged_results, threshold
        )

    return merged_results
