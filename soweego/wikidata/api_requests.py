#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of specific Web API requests for Wikidata data collection."""
from soweego.commons.db_manager import DBManager

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
import os
import pickle
from collections import defaultdict
from functools import lru_cache, partial
from multiprocessing.pool import Pool
from typing import Dict, Iterator, List, Set, TextIO, Tuple, Union
from urllib.parse import urlunsplit

import requests
from requests.exceptions import RequestException
from tqdm import tqdm

from soweego.commons import constants, keys
from soweego.commons.logging import log_request_data
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
BUCKET_SIZE = 500


def get_biodata(qids: Set[str]) -> Iterator[Tuple[str, str, str]]:
    """Collect biographical data for a given set of Wikidata items.

    :param qids: a set of QIDs
    :return: the generator yielding ``(QID, PID, value)`` triples
    """
    no_claims_count = 0
    qid_buckets, request_params = _prepare_request(qids, 'claims')

    for bucket in qid_buckets:
        entities = _sanity_check(bucket, request_params)

        if entities is None:
            continue

        for qid in entities:
            claims = entities[qid].get('claims')
            if not claims:
                LOGGER.info('Skipping QID with no claims: %s', qid)
                no_claims_count += 1
                continue

            yield from _yield_expected_values(
                qid,
                claims,
                vocabulary.BIODATA_PIDS,
                no_claims_count,
                include_pid=True,
            )

    LOGGER.info(
        'Got %d QIDs with no %s claims',
        no_claims_count,
        vocabulary.BIODATA_PIDS,
    )


def get_links(
        qids: Set[str], url_pids: Set[str], ext_id_pids_to_urls: Dict
) -> Iterator[Tuple]:
    """Collect sitelinks and third-party links
    for a given set of Wikidata items.

    :param qids: a set of QIDs
    :param url_pids: a set of PIDs holding URL values.
      Returned by :py:func:`soweego.wikidata.sparql_queries.url_pids`
    :param ext_id_pids_to_urls: a
      ``{PID: {formatter_URL: formatter_regex} }`` dict.
      Returned by
      :py:func:`soweego.wikidata.sparql_queries.external_id_pids_and_urls`
    :return: the generator yielding ``(QID, URL)`` pairs
    """
    no_sitelinks_count, no_links_count, no_ext_ids_count = 0, 0, 0
    qid_buckets, request_params = _prepare_request(qids, 'sitelinks|claims')

    for bucket in qid_buckets:
        entities = _sanity_check(bucket, request_params)

        if entities is None:
            continue

        for qid in entities:
            entity = entities[qid]

            # Sitelinks
            yield _yield_sitelinks(entity, qid, no_sitelinks_count)

            claims = entity.get('claims')
            if claims:
                # Third-party links
                yield _yield_expected_values(
                    qid, claims, url_pids, no_links_count
                )

                # External ID links
                yield _yield_ext_id_links(
                    ext_id_pids_to_urls, claims, qid, no_ext_ids_count
                )
            else:
                LOGGER.info('No claims for QID %s', qid)

    LOGGER.info(
        'QIDs: got %d with no sitelinks, '
        '%d with no third-party links, '
        '%d with no external ID links',
        no_sitelinks_count,
        no_links_count,
        no_ext_ids_count,
    )


def get_data_for_linker(
        catalog: str,
        entity: str,
        qids: Set[str],
        url_pids: Set[str],
        ext_id_pids_to_urls: Dict,
        qids_and_tids: Dict,
        fileout: TextIO,
) -> None:
    """Collect relevant data for linking Wikidata to a given catalog.
    Dump the result to a given output stream.

    This function uses multithreaded parallel processing.

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param qids: a set of QIDs
    :param url_pids: a set of PIDs holding URL values.
      Returned by :py:func:`soweego.wikidata.sparql_queries.url_pids`
    :param ext_id_pids_to_urls: a
      ``{PID: {formatter_URL: formatter_regex} }`` dict.
      Returned by
      :py:func:`soweego.wikidata.sparql_queries.external_id_pids_and_urls`
    :param fileout: a file stream open for writing
    :param qids_and_tids: a ``{QID: {'tid': {catalog_ID_set} }`` dict.
      Populated by
      :py:func:`soweego.commons.data_gathering.gather_target_ids`
    """
    qid_buckets, request_params = _prepare_request(
        qids, 'labels|aliases|descriptions|sitelinks|claims'
    )

    # Catalog-specific data needs
    if catalog in constants.REQUIRE_OCCUPATION.keys():
        needs_occupation = entity in constants.REQUIRE_OCCUPATION[catalog]
    else:
        needs_occupation = False
    needs_genre = entity in constants.REQUIRE_GENRE
    needs_publication_date = entity in constants.REQUIRE_PUBLICATION_DATE

    # Initialize 7 counters to 0
    # Indices legend:
    # 0 = claims
    # 1 = labels
    # 2 = aliases
    # 3 = descriptions
    # 4 = sitelinks
    # 5 = third-party URLs
    # 6 = third-party IDs
    counters = [0] * 7

    # Create a partial function where all parameters
    # but the data bucket are passed to `_process_bucket`,
    # so that we only pass the data bucket
    # when we call `pool_function`.
    # In this way, it becomes trivial to use
    # `multiprocessing.Pool` map functions, like `imap_unordered`
    pool_function = partial(
        _process_bucket,
        request_params=request_params,
        url_pids=url_pids,
        ext_id_pids_to_urls=ext_id_pids_to_urls,
        qids_and_tids=qids_and_tids,
        needs=(needs_occupation, needs_genre, needs_publication_date),
        counters=counters,
    )

    # Create a pool of threads and map the list of buckets via `pool_function`
    with Pool() as pool:
        # `processed_bucket` will be a list of dicts, where each dict
        # is a processed entity from the bucket
        for processed_bucket in pool.imap_unordered(
                pool_function, tqdm(qid_buckets, total=len(qid_buckets))
        ):
            # Join results into a string so that we can write them to
            # the dump file
            to_write = ''.join(
                json.dumps(result, ensure_ascii=False) + '\n'
                for result in processed_bucket
            )

            fileout.write(to_write)
            fileout.flush()

    LOGGER.info(
        'QIDs: got %d with no expected claims, %d with no labels, '
        '%d with no aliases, %d with no descriptions, %d with no sitelinks, '
        '%d with no third-party links, %d with no external ID links',
        *counters
    )


@lru_cache()
def build_session() -> requests.Session:
    """Build the HTTP session for interaction with the Wikidata API.

    Log in if credentials are found,
    otherwise go ahead with an unauthenticated session.
    If a previously cached session has expired, build a new one.

    :rtype: :py:class:`requests.Session`
    :return: the HTTP session to interact with the Wikidata API
    """
    session_dump_path = os.path.join(
        constants.SHARED_FOLDER, constants.WIKIDATA_API_SESSION
    )

    try:
        return _load_cached_session(session_dump_path)

    except (FileNotFoundError, AssertionError):
        LOGGER.debug('Logging into the Wikidata API ...')
        try:
            # Try to login by loading credentials from file
            success, err_msg, session = _login(*_get_credentials_from_file())

            # Login failed: wrong user and/or password.
            # Stop execution
            if not success:
                raise AssertionError(err_msg)

        except (FileNotFoundError, KeyError) as error:
            if isinstance(error, FileNotFoundError):
                LOGGER.info(
                    "Credentials file not found, "
                    "won't log into the Wikidata API. "
                    "Please put '%s' in the '%s' module "
                    "if you want to log in next time",
                    constants.CREDENTIALS_FILENAME,
                    constants.CREDENTIALS_MODULE,
                )
            elif isinstance(error, KeyError):
                LOGGER.info(
                    "No %s found in the credentials file, "
                    "won't log into the Wikidata API. "
                    "Please add it to '%s' in the '%s' module "
                    "if you want to log in next time",
                    error,
                    constants.CREDENTIALS_FILENAME,
                    constants.CREDENTIALS_MODULE,
                )

            global BUCKET_SIZE
            BUCKET_SIZE = 50

            # Don't persist an unauthenticated session
            return requests.Session()

        with open(session_dump_path, 'wb') as file:
            LOGGER.debug('Login successful, persisting session to disk ...')
            pickle.dump(session, file)

        return session


def parse_value(
        value: Union[str, Dict]
) -> Union[str, Tuple[str, str], Set[str], None]:
    """Parse a value returned by the Wikidata API into standard Python objects.

    The parser supports the following Wikidata
    `data types <https://www.wikidata.org/wiki/Special:ListDatatypes>`_:

    - string > *str*
    - URL > *str*
    - monolingual text > *str*
    - time > *tuple* ``(time, precision)``
    - item > *set* ``{item_labels}``

    :param value: a data value from a call to the Wikidata API
    :return: the parsed Python object, or ``None`` if parsing failed
    """
    # Plain string
    if isinstance(value, str):
        return value

    # Monolingual string
    monolingual_string_value = value.get('text')
    if monolingual_string_value:
        return monolingual_string_value

    # Date: return tuple (date, precision)
    date_value = value.get('time')
    if date_value and date_value.startswith('-'):  # Drop BC support
        LOGGER.warning(
            'Cannot parse BC (Before Christ) date, Python does not support it: %s',
            date_value,
        )
        return None
    if date_value:
        return date_value[1:], value['precision']  # Get rid of leading '+'

    # QID: return set of labels
    qid_value = value.get('id')
    if qid_value:
        return _lookup_label(qid_value)

    LOGGER.warning('Failed parsing value: %s', value)
    return None


def _sanity_check(bucket, request_params):
    response_body = _make_request(bucket, request_params)
    # Failed API request
    if not response_body:
        return None

    entities = response_body.get('entities')
    # Unexpected JSON response
    if not entities:
        LOGGER.warning(
            'Skipping unexpected JSON response with no entities: %s',
            response_body,
        )
        return None

    return entities


def _lookup_label(item_value):
    request_params = {
        'action': 'wbgetentities',
        'format': 'json',
        'props': 'labels',
    }
    response_body = _make_request([item_value], request_params)
    if not response_body:
        LOGGER.warning('Failed label lookup for %s', item_value)
        return None
    labels = response_body['entities'][item_value].get('labels')
    if not labels:
        LOGGER.info('No label for %s', item_value)
        return None

    return _return_monolingual_strings(item_value, labels)


# This function will be consumed by `get_data_for_linker`:
# it enables parallel processing for Wikidata buckets
def _process_bucket(
        bucket,
        request_params,
        url_pids,
        ext_id_pids_to_urls,
        qids_and_tids,
        needs,
        counters,
) -> List[Dict]:
    entities = _sanity_check(bucket, request_params)

    # If the sanity check went wrong,
    # we treat the bucket as if there were no entities,
    # and return an empty list
    if entities is None:
        return []

    result = []

    for qid in entities:
        processed = {}

        # Stick target IDs if given
        if qids_and_tids:
            tids = qids_and_tids.get(qid)
            if tids:
                processed[keys.TID] = list(tids[keys.TID])

        entity = entities[qid]

        # Claims
        claims = entity.get('claims')
        if not claims:
            LOGGER.info('Skipping QID with no claims: %s', qid)
            counters[0] += 1
            continue

        # Labels
        labels = entity.get('labels')
        if not labels:
            LOGGER.info('Skipping QID with no labels: %s', qid)
            counters[1] += 1
            continue
        processed[keys.QID] = qid
        processed[keys.NAME] = _return_monolingual_strings(qid, labels)

        # Aliases
        aliases = entity.get('aliases')
        if aliases:
            # Merge them into labels
            processed[keys.NAME].update(_return_aliases(qid, aliases))
        else:
            LOGGER.debug('%s has no aliases', qid)
            counters[2] += 1
        # Convert set to list for JSON serialization
        processed[keys.NAME] = list(processed[keys.NAME])

        # Descriptions
        descriptions = entity.get('descriptions')
        if descriptions:
            processed[keys.DESCRIPTION] = list(
                _return_monolingual_strings(qid, descriptions)
            )
        else:
            LOGGER.debug('%s has no descriptions', qid)
            counters[3] += 1

        # Sitelinks
        sitelinks = entity.get('sitelinks')
        if sitelinks:
            processed[keys.URL] = _return_sitelinks(sitelinks)
        else:
            LOGGER.debug('%s has no sitelinks', qid)
            processed[keys.URL] = set()
            counters[4] += 1

        # Third-party URLs
        processed[keys.URL].update(
            _return_third_party_urls(qid, claims, url_pids, counters)
        )

        # External ID URLs
        processed[keys.URL].update(
            _return_ext_id_urls(qid, claims, ext_id_pids_to_urls, counters)
        )
        # Convert set to list for JSON serialization
        processed[keys.URL] = list(processed[keys.URL])

        # Expected claims
        processed.update(
            _return_claims_for_linker(qid, claims, needs, counters)
        )

        result.append(processed)

    return result


def _return_monolingual_strings(qid, strings):
    # Language codes are discarded, since we opt for
    # language-agnostic feature extraction.
    # See soweego.linker.workflow#extract_features
    to_return = set()

    for data in strings.values():
        string = data.get('value')

        if not string:
            LOGGER.warning(
                'Skipping malformed monolingual string with no value for %s: %s',
                qid,
                data,
            )
            continue

        to_return.add(string)

    return to_return


def _return_aliases(qid, aliases):
    # Language codes are discarded, since we opt for
    # language-agnostic feature extraction.
    # See soweego.linker.workflow#extract_features
    to_return = set()

    for values in aliases.values():
        for data in values:
            alias = data.get('value')

            if not alias:
                LOGGER.warning(
                    'Skipping malformed alias with no value for %s: %s',
                    qid,
                    data,
                )
                continue

            to_return.add(alias)

    return to_return


def _return_sitelinks(sitelinks):
    to_return = set()

    for site, data in sitelinks.items():
        to_return.add(_build_sitelink_url(site, data['title']))

    return to_return


def _return_third_party_urls(qid, claims, url_pids, counters):
    to_return = set()
    available = url_pids.intersection(claims.keys())

    if available:
        LOGGER.debug(
            'Available third-party URL PIDs for %s: %s', qid, available
        )
        for pid in available:
            for pid_claim in claims[pid]:
                value = _extract_value_from_claim(pid_claim, pid, qid)

                if not value:
                    continue

                parsed_value = parse_value(value)

                if not parsed_value:
                    continue

                to_return.add(parsed_value)
    else:
        LOGGER.debug('No third-party URLs for %s', qid)
        counters[5] += 1

    return to_return


def _return_claims_for_linker(qid, claims, needs, counters):
    # Unpack needs
    needs_occupation, needs_genre, needs_publication_date = needs
    to_return = defaultdict(set)
    expected_pids = set(vocabulary.LINKER_PIDS.keys())

    if not needs_occupation:
        expected_pids.remove(vocabulary.OCCUPATION)

    if not needs_genre:
        expected_pids.remove(vocabulary.GENRE)

    # If we need publication dates, it means we are dealing
    # with works, so remove birth and death dates
    if needs_publication_date:
        expected_pids.remove(vocabulary.DATE_OF_BIRTH)
        expected_pids.remove(vocabulary.DATE_OF_DEATH)
    else:
        expected_pids.remove(vocabulary.PUBLICATION_DATE)

    available = expected_pids.intersection(claims.keys())

    if available:
        LOGGER.debug('Available claim PIDs for %s: %s', qid, available)
        for pid in available:
            for pid_claim in claims[pid]:
                handled = _handle_expected_claims(
                    expected_pids, qid, pid, pid_claim, to_return
                )

                if not handled:
                    continue

    else:
        LOGGER.debug('No %s expected claims for %s', expected_pids, qid)
        counters[0] += 1

    return {field: list(values) for field, values in to_return.items()}


def _handle_expected_claims(expected_pids, qid, pid, pid_claim, to_return):
    value = _extract_value_from_claim(pid_claim, pid, qid)
    if not value:
        return False

    pid_label = vocabulary.LINKER_PIDS.get(pid)
    if not pid_label:
        LOGGER.critical(
            'PID label lookup failed: %s. The PID should be one of %s',
            pid,
            expected_pids,
        )
        raise ValueError(
            'PID label lookup failed: %s. The PID should be one of %s'
            % (pid, expected_pids)
        )

    if pid == vocabulary.OCCUPATION:
        # for occupations we only need their QID
        # so we add it to `to_return` and continue,
        # since we don't need to extract labels
        parsed_value = value.get('id')
    else:
        parsed_value = parse_value(value)

    if not parsed_value:
        return False

    if isinstance(parsed_value, set):  # Labels
        to_return[pid_label].update(parsed_value)
    else:
        to_return[pid_label].add(parsed_value)

    return True


def _return_ext_id_urls(qid, claims, ext_id_pids_to_urls, counters):
    to_return = set()
    available = set(ext_id_pids_to_urls.keys()).intersection(claims.keys())

    if available:
        LOGGER.debug('Available external ID PIDs for %s: %s', qid, available)
        for pid in available:
            for pid_claim in claims[pid]:
                ext_id = _extract_value_from_claim(pid_claim, pid, qid)

                if not ext_id:
                    continue

                for formatter_url in ext_id_pids_to_urls[pid]:
                    to_return.add(formatter_url.replace('$1', ext_id))
    else:
        LOGGER.debug('No external ID links for %s', qid)
        counters[6] += 1

    return to_return


def _yield_sitelinks(entity, qid, no_sitelinks_count):
    sitelinks = entity.get('sitelinks')

    if not sitelinks:
        LOGGER.debug('No sitelinks for %s', qid)
        no_sitelinks_count += 1
    else:
        LOGGER.debug('Sitelinks for %s: %s', qid, sitelinks)
        for site, data in sitelinks.items():
            url = _build_sitelink_url(site, data['title'])
            yield qid, url


def _yield_ext_id_links(ext_id_pids_to_urls, claims, qid, no_ext_ids_count):
    available_ext_id_pids = set(ext_id_pids_to_urls.keys()).intersection(
        claims.keys()
    )

    if not available_ext_id_pids:
        LOGGER.debug('No external identifier links for %s', qid)
        no_ext_ids_count += 1
    else:
        LOGGER.debug(
            'Available PIDs with external IDs for %s: %s',
            qid,
            available_ext_id_pids,
        )
        for pid in available_ext_id_pids:
            for pid_claim in claims[pid]:
                ext_id = _extract_value_from_claim(pid_claim, pid, qid)

                if not ext_id:
                    continue

                for formatter_url in ext_id_pids_to_urls[pid]:
                    yield qid, formatter_url.replace('$1', ext_id)


def _yield_expected_values(
        qid, claims, expected_pids, count, include_pid=False
):
    available = expected_pids.intersection(claims.keys())

    if not available:
        LOGGER.debug('No %s expected claims for %s', expected_pids, qid)
        count += 1
    else:
        LOGGER.debug('Available claims for %s: %s', qid, available)
        for pid in available:
            for pid_claim in claims[pid]:
                value = _extract_value_from_claim(pid_claim, pid, qid)

                if not value:
                    continue

                if include_pid:
                    yield qid, pid, value
                else:
                    yield qid, value


def _prepare_request(qids, props):
    build_session()
    qid_buckets = _make_buckets(qids)
    request_params = {
        'action': 'wbgetentities',
        'format': 'json',
        'props': props,
    }
    return qid_buckets, request_params


# API login step 1:
# get the login token, using the given HTTP session
def _get_login_token(session: requests.Session) -> str:
    token_response = session.get(
        WIKIDATA_API_URL,
        params={
            'action': 'query',
            'meta': 'tokens',
            'type': 'login',
            'format': 'json',
        },
        headers={'User-Agent': constants.HTTP_USER_AGENT},
    ).json()

    return token_response['query']['tokens']['logintoken']


# API login step 2:
# actual login with the given token and password,
# using the given HTTP session.
# Return whether the login was successful or not,
# and an eventual error message from the server.
# Cookies for authentication are automatically saved into the session.
def _actual_login(
        session: requests.Session, user: str, password: str, token: str
) -> Tuple[bool, str]:
    login_response = session.post(
        WIKIDATA_API_URL,
        data={
            'action': 'login',
            'lgname': user,
            'lgpassword': password,
            'lgtoken': token,
            'format': 'json',
        },
        headers={'User-Agent': constants.HTTP_USER_AGENT},
    ).json()

    success = login_response['login']['result'] != 'Failed'

    # None in case of successful login
    err_msg = login_response['login'].get('reason')

    return success, err_msg


# Load the pickled bot session, check if it's valid,
# then return the session or raise `AssertionError`.
# Raise `FileNotFoundError` if the pickle file doesn't exist
def _load_cached_session(dump_path: str) -> requests.Session:
    with open(dump_path, 'rb') as file:
        LOGGER.debug('Loading authenticated session ...')
        session = pickle.load(file)

        # Check if the session is still valid
        assert_response = session.get(
            WIKIDATA_API_URL,
            params={'action': 'query', 'assert': 'user', 'format': 'json'},
            headers={'User-Agent': constants.HTTP_USER_AGENT},
        )

        # If the assert request failed,
        # we need to renew the session
        if 'error' in assert_response.json().keys():
            LOGGER.info('The session has expired and will be renewed')
            raise AssertionError

        return session


def _login(user: str, password: str) -> Tuple[bool, str, requests.Session]:
    session = requests.Session()  # To automatically manage cookies
    token = _get_login_token(session)
    success, err_msg = _actual_login(session, user, password, token)

    return success, err_msg, session


# Raise `FileNotFoundError` if the JSON file is not there
# Raise `KeyError` if credential keys are not in the JSON file
def _get_credentials_from_file() -> Tuple[Union[str, None], Union[str, None]]:
    credentials = DBManager.get_credentials()
    return (
        credentials[keys.WIKIDATA_API_USER],
        credentials[keys.WIKIDATA_API_PASSWORD],
    )


def _make_request(bucket, params):
    params['ids'] = '|'.join(bucket)
    session = requests.Session()
    session.cookies = build_session().cookies

    while True:
        response = None
        try:
            response = session.get(
                WIKIDATA_API_URL,
                params=params,
                headers={'User-Agent': constants.HTTP_USER_AGENT},
            )
            log_request_data(response, LOGGER)
        except (RequestException, Exception) as error:
            if isinstance(error, RequestException):
                LOGGER.warning(
                    'Connection broken, retrying the request to the Wikidata API'
                )
            else:
                LOGGER.error(
                    'Unexpected error, retrying the request to '
                    'the Wikidata API anyway. '
                    'Reason: %s',
                    error,
                )
            connection_is_ok = False
        else:
            connection_is_ok = True

        if connection_is_ok:
            break

    if not response.ok or response is None:
        LOGGER.warning(
            'Skipping failed %s to the Wikidata API. Reason: %d %s - Full URL: %s',
            response.request.method,
            response.status_code,
            response.reason,
            response.request.url,
        )
        return None

    LOGGER.debug(
        'Successful %s to the Wikidata API. Status code: %d',
        response.request.method,
        response.status_code,
    )
    return response.json()


def _extract_value_from_claim(pid_claim, pid, qid):
    LOGGER.debug('Processing (%s, %s) claim: %s', qid, pid, pid_claim)
    main_snak = pid_claim.get('mainsnak')
    if not main_snak:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no main snak', qid, pid
        )
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    snak_type = main_snak.get('snaktype')
    if not snak_type:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no snak type', qid, pid
        )
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    if snak_type == 'novalue':
        LOGGER.warning(
            "Skipping unexpected (%s, %s) claim with 'novalue' snak type",
            qid,
            pid,
        )
        LOGGER.debug("Unexpected claim with 'novalue' snak type: %s", pid_claim)
        return None
    data_value = main_snak.get('datavalue')
    if not data_value:
        LOGGER.warning(
            "Skipping unexpected (%s, %s) claim with no 'datavalue'", qid, pid
        )
        LOGGER.debug("Unexpected claim with no 'datavalue': %s", pid_claim)
        return None
    value = data_value.get('value')
    if not value:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no value', qid, pid
        )
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    LOGGER.debug('QID: %s - PID: %s - Value: %s', qid, pid, value)
    return value


def _build_sitelink_url(site, title):
    netloc_builder = []
    split_index = site.find('wiki')
    language = site[:split_index]
    netloc_builder.append(language.replace('_', '-'))
    project = site[split_index:]
    if project == 'wiki':
        project = 'wikipedia'
    if language == 'commons':
        project = 'wikimedia'
    netloc_builder.append(project)
    netloc_builder.append('org')
    url = urlunsplit(
        (
            'https',
            '.'.join(netloc_builder),
            '/wiki/%s' % title.replace(' ', '_'),
            '',
            '',
        )
    )
    LOGGER.debug('Site: %s - Title: %s - Full URL: %s', site, title, url)
    return url


def _make_buckets(qids):
    buckets = []
    current_bucket = []
    for qid in qids:
        current_bucket.append(qid)
        if len(current_bucket) >= BUCKET_SIZE:
            buckets.append(current_bucket)
            current_bucket = []
    buckets.append(current_bucket)
    LOGGER.info(
        'Made %d buckets of size %d out of %d QIDs to comply with the Wikidata API limits',
        len(buckets),
        BUCKET_SIZE,
        len(qids),
    )
    return buckets
