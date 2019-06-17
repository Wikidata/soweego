#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of specific requests for the Wikidata API:
https://www.wikidata.org/w/api.php
"""

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
from pkgutil import get_data
from typing import Dict, Generator, List, TextIO, Tuple
from urllib.parse import urlunsplit

import requests
from requests.exceptions import ChunkedEncodingError
from tqdm import tqdm

from soweego.commons import constants, keys
from soweego.commons.logging import log_request_data
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
BUCKET_SIZE = 500


# Values: plain strings (includes URLs), monolingual strings,
# birth/death dates, QIDs (gender, birth/death places)
def parse_wikidata_value(value):
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


def _process_bucket(
    bucket,
    request_params,
    url_pids,
    ext_id_pids_to_urls,
    qids_and_tids,
    no_labels_count,
    no_aliases_count,
    no_descriptions_count,
    no_sitelinks_count,
    no_links_count,
    no_ext_ids_count,
    no_claims_count,
    needs_occupation,
    needs_genre,
    needs_publication_date,
) -> List[Dict]:
    """
    This function will be consumed by the `get_data_for_linker`
    function in this module. It allows the buckets coming from
    wikidata to be processed in parallel.
    """
    response_body = _make_request(bucket, request_params)

    # If there is no response then
    # we treat it as if there were no entities in
    # the bucket, and return an empty list
    if not response_body:
        return []

        # Each bucket is composed of different entities.
    # In this list we'll keep track of the results
    # when processing each of them
    bucket_results = []

    for qid in response_body['entities']:
        to_write = {}

        # Stick target ids if given
        if qids_and_tids:
            tids = qids_and_tids.get(qid)
            if tids:
                to_write[keys.TID] = list(tids[keys.TID])

        entity = response_body['entities'][qid]
        claims = entity.get('claims')
        if not claims:
            LOGGER.info('Skipping QID with no claims: %s', qid)
            no_claims_count += 1
            continue

        # Labels
        labels = entity.get('labels')
        if not labels:
            LOGGER.info('Skipping QID with no labels: %s', qid)
            no_labels_count += 1
            continue
        to_write[keys.QID] = qid
        to_write[keys.NAME] = _return_monolingual_strings(qid, labels)

        # Aliases
        aliases = entity.get('aliases')
        if aliases:
            # Merge them into labels
            to_write[keys.NAME].update(_return_aliases(qid, aliases))
        else:
            LOGGER.debug('%s has no aliases', qid)
            no_aliases_count += 1
        # Convert set to list for JSON serialization
        to_write[keys.NAME] = list(to_write[keys.NAME])

        # Descriptions
        descriptions = entity.get('descriptions')
        if descriptions:
            to_write[keys.DESCRIPTION] = list(
                _return_monolingual_strings(qid, descriptions)
            )
        else:
            LOGGER.debug('%s has no descriptions', qid)
            no_descriptions_count += 1

        # Sitelinks
        sitelinks = entity.get('sitelinks')
        if sitelinks:
            to_write[keys.URL] = _return_sitelinks(sitelinks)
        else:
            LOGGER.debug('%s has no sitelinks', qid)
            to_write[keys.URL] = set()
            no_sitelinks_count += 1

        # Third-party URLs
        to_write[keys.URL].update(
            _return_third_party_urls(qid, claims, url_pids, no_links_count)
        )

        # External ID URLs
        to_write[keys.URL].update(
            _return_ext_id_urls(
                qid, claims, ext_id_pids_to_urls, no_ext_ids_count
            )
        )
        # Convert set to list for JSON serialization
        to_write[keys.URL] = list(to_write[keys.URL])

        # Expected claims
        to_write.update(
            _return_claims_for_linker(
                qid,
                claims,
                no_claims_count,
                needs_occupation,
                needs_genre,
                needs_publication_date,
            )
        )

        # add result to `bucket_results`
        bucket_results.append(to_write)

    return bucket_results


def get_data_for_linker(
    catalog: str,
    entity_type: str,
    qids: set,
    url_pids: set,
    ext_id_pids_to_urls: dict,
    fileout: TextIO,
    qids_and_tids: dict,
) -> None:
    no_labels_count = 0
    no_aliases_count = 0
    no_descriptions_count = 0
    no_sitelinks_count = 0
    no_links_count = 0
    no_ext_ids_count = 0
    no_claims_count = 0

    qid_buckets, request_params = _prepare_request(
        qids, 'labels|aliases|descriptions|sitelinks|claims'
    )

    # check if for this specific catalog
    # we need to get the occupations
    if catalog in constants.REQUIRE_OCCUPATION.keys():
        needs_occupation = entity_type in constants.REQUIRE_OCCUPATION[catalog]
    else:
        needs_occupation = False
    needs_genre = entity_type in constants.REQUIRE_GENRE
    needs_publication_date = entity_type in constants.REQUIRE_PUBLICATION_DATE

    # create a partial function. Here all parameters, except
    # for the actual `bucket` are given to `_process_bucket`.
    # This means that when we call `pool_function` we only need
    # to give it one parameter, which is the bucket.
    # This is done so that we can easily map the list of
    # buckets with this function using `multiprocessing.Pool`
    pool_function = partial(
        _process_bucket,
        request_params=request_params,
        url_pids=url_pids,
        ext_id_pids_to_urls=ext_id_pids_to_urls,
        qids_and_tids=qids_and_tids,
        no_labels_count=no_labels_count,
        no_aliases_count=no_aliases_count,
        no_descriptions_count=no_descriptions_count,
        no_sitelinks_count=no_sitelinks_count,
        no_links_count=no_links_count,
        no_ext_ids_count=no_ext_ids_count,
        no_claims_count=no_claims_count,
        needs_occupation=needs_occupation,
        needs_genre=needs_genre,
        needs_publication_date=needs_publication_date,
    )

    # create a pool of threads and map the list of buckets using `pool_function`
    with Pool() as pool:
        # `processed_bucket` will be a list of dicts, where each dict
        # is a processed entity from the bucket
        for processed_bucket in pool.imap_unordered(
            pool_function, tqdm(qid_buckets, total=len(qid_buckets))
        ):
            # join results into a string so that we can write them to
            # the dump file
            to_write = ''.join(
                json.dumps(result, ensure_ascii=False) + '\n'
                for result in processed_bucket
            )

            fileout.write(to_write)
            fileout.flush()

    LOGGER.info(
        'QIDs: got %d with no labels, %d with no aliases, %d with no descriptions, %d with no sitelinks, %d with no third-party links, %d with no external ID links, %d with no expected claims',
        no_labels_count,
        no_aliases_count,
        no_descriptions_count,
        no_sitelinks_count,
        no_links_count,
        no_ext_ids_count,
        no_claims_count,
    )


def get_metadata(qids: set) -> Generator[tuple, None, None]:
    no_claims_count = 0

    qid_buckets, request_params = _prepare_request(qids, 'claims')
    for bucket in qid_buckets:
        response_body = _make_request(bucket, request_params)
        if not response_body:
            continue
        for qid in response_body['entities']:
            claims = response_body['entities'][qid].get('claims')
            if not claims:
                LOGGER.info('Skipping QID with no claims: %s', qid)
                no_claims_count += 1
                continue
            # Remember this yields a generator of generators
            # see https://stackoverflow.com/questions/6503079/understanding-nested-yield-return-in-python#6503192
            yield _yield_expected_values(
                qid,
                claims,
                vocabulary.METADATA_PIDS,
                no_claims_count,
                include_pid=True,
            )

    LOGGER.info(
        'Got %d QIDs with no %s claims',
        no_claims_count,
        vocabulary.METADATA_PIDS,
    )


def get_links(
    qids: set, url_pids: set, ext_id_pids_to_urls: dict
) -> Generator[tuple, None, None]:
    """Get sitelinks and third-party links for each Wikidata item in the given set.

    :param qids: set of Wikidata QIDs
    :type qids: set
    :param url_pids: set of Wikidata PIDs having a URL as expected value
    :type url_pids: set
    :param ext_id_pids_to_urls: a dictionary ``{external_ID_PID: {formatter_URL: formatter_regex}}``
    :type ext_id_pids_to_urls: dict
    :return: a generator yielding ``QID, URL`` tuples
    :rtype: Generator[tuple, None, None]
    """
    no_sitelinks_count = 0
    no_links_count = 0
    no_ext_ids_count = 0

    qid_buckets, request_params = _prepare_request(qids, 'sitelinks|claims')
    for bucket in qid_buckets:
        response_body = _make_request(bucket, request_params)
        if not response_body:
            continue
        for qid in response_body['entities']:
            entity = response_body['entities'][qid]
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
                LOGGER.warning('No claims for QID %s', qid)

    LOGGER.info(
        'QIDs: got %d with no sitelinks, %d with no third-party links, %d with no external ID links',
        no_sitelinks_count,
        no_links_count,
        no_ext_ids_count,
    )


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


def _return_third_party_urls(qid, claims, url_pids, no_count):
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
                parsed_value = parse_wikidata_value(value)
                if not parsed_value:
                    continue
                to_return.add(parsed_value)
    else:
        LOGGER.debug('No third-party URLs for %s', qid)
        no_count += 1
    return to_return


def _return_claims_for_linker(
    qid, claims, no_count, needs_occupation, needs_genre, needs_publication_date
):
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

                value = _extract_value_from_claim(pid_claim, pid, qid)
                if not value:
                    continue

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
                    parsed_value = parse_wikidata_value(value)

                if not parsed_value:
                    continue

                if isinstance(parsed_value, set):  # Labels
                    to_return[pid_label].update(parsed_value)

                else:
                    to_return[pid_label].add(parsed_value)

    else:
        LOGGER.debug('No %s expected claims for %s', expected_pids, qid)
        no_count += 1
    return {field: list(values) for field, values in to_return.items()}


def _return_ext_id_urls(qid, claims, ext_id_pids_to_urls, no_count):
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
        no_count += 1
    return to_return


def _yield_monolingual_strings(qid, strings, string_type):
    for language_code, data in strings.items():
        string = data.get('value')
        if not string:
            LOGGER.warning(
                'Skipping malformed monolingual string with no value for %s: %s',
                qid,
                data,
            )
            continue
        yield qid, language_code, string, string_type


def _yield_aliases(qid, aliases):
    for language_code, values in aliases.items():
        for data in values:
            alias = data.get('value')
            if not alias:
                LOGGER.warning(
                    'Skipping malformed alias with no value for %s: %s',
                    qid,
                    data,
                )
                continue
            yield qid, language_code, alias, keys.ALIAS


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
    qid_buckets = _make_buckets(qids)
    request_params = {
        'action': 'wbgetentities',
        'format': 'json',
        'props': props,
    }
    return qid_buckets, request_params


def _get_authentication_token(session: requests.Session) -> str:
    """
    Using a session instance, get a token we can use for authentication
    """
    token_request = session.get(
        WIKIDATA_API_URL,
        params={
            'action': 'query',
            'meta': 'tokens',
            'type': 'login',
            'format': 'json',
        },
        headers={'User-Agent': constants.HTTP_USER_AGENT},
    ).json()

    return token_request['query']['tokens']['logintoken']


def _do_bot_login(
    session: requests.Session, token: str, bot_password: str
) -> bool:
    """
    Tries to login with a session, given token and password. Returns a boolean
    stating whether the login was successful or not.

    Cookies for authentication are automatically saved into the session.
    """

    login_r = session.post(
        WIKIDATA_API_URL,
        data={
            'action': 'login',
            'lgname': 'Soweego bot',
            'lgpassword': bot_password,
            'lgtoken': token,
            'format': 'json',
        },
        headers={'User-Agent': constants.HTTP_USER_AGENT},
    ).json()

    lg_success = login_r['login']['result'] != 'Failed'

    # message is None when login is successful
    lg_message = login_r['login'].get('reason', None)

    return lg_success, lg_message


def _load_cached_bot_session(dump_path: str) -> requests.Session:
    """
    Loads the pickled bot session checks if it is valid and returns session.

    Raises `AssertionError` if session is not valid
    Raises `FileNotFoundError` if path doesn't exist
    """

    with open(dump_path, 'rb') as file:
        LOGGER.debug('Previously authenticated session exists')
        session = pickle.load(file)

        # check if session is still valid
        res = session.get(
            WIKIDATA_API_URL,
            params={'action': 'query', 'assert': 'user', 'format': 'json'},
            headers={'User-Agent': constants.HTTP_USER_AGENT},
        )

        # if the assert query failed then it means
        # we need to renew the session
        if 'error' in res.json().keys():
            LOGGER.info('Session has expired and must be renewed')
            raise AssertionError

        return session


def _authenticate_session(bot_password) -> Tuple[bool, str, requests.Session]:
    """
    Creates an authenticated session using the given password
    """

    session = requests.Session()  # to automatically manage cookies

    # get token
    token = _get_authentication_token(session)

    # do login
    lg_success, lg_message = _do_bot_login(session, token, bot_password)

    return lg_success, lg_message, session


def _get_bot_password_from_file() -> str:
    """
    Get the password for the wikidata bot from the `db_credentials.json` file.

    May raise a :py:class:`KeyError` exception if the key is not present.
    """

    return json.loads(get_data(*constants.CREDENTIALS_LOCATION))[
        keys.WIKIDATA_BOT_PASSWORD
    ]


@lru_cache()
def get_authenticated_session():
    """
    Returns the token to be used for authentication.
    If token is not valid then a new one will be generated
    """

    wiki_api_dump_path = os.path.join(
        constants.SHARED_FOLDER, constants.WIKIDATA_API_SESSION
    )

    try:
        return _load_cached_bot_session(wiki_api_dump_path)

    except (FileNotFoundError, AssertionError):
        LOGGER.info('Obtaining new authenticated session')

        try:
            # Try to load the password from file and create
            # an authenticated session with it
            success, msg, session = _authenticate_session(
                _get_bot_password_from_file()
            )

            # If we weren't able to login then it must mean that
            # the password we have in file is not correct.
            # Execution should not proceed
            if not success:
                raise AssertionError(msg)

        except KeyError:
            LOGGER.info(
                'No password found in file, proceeding with an unauthenticated session'
            )

            global BUCKET_SIZE
            BUCKET_SIZE = 50

            # we return the session at once since we don't
            # want to persist an unauthenticated session to disk
            return requests.Session()

        with open(wiki_api_dump_path, 'wb') as file:
            LOGGER.info('Authentication successful, persisting session to disk')
            pickle.dump(session, file)

        return session


def _make_request(bucket, params):
    params['ids'] = '|'.join(bucket)
    connection_is_ok = True

    session = requests.Session()
    session.cookies = get_authenticated_session().cookies

    while True:
        try:
            response = session.get(
                WIKIDATA_API_URL,
                params=params,
                headers={'User-Agent': constants.HTTP_USER_AGENT},
            )
            log_request_data(response, LOGGER)
        except ChunkedEncodingError:
            LOGGER.warning(
                'Connection broken, retrying the request to the Wikidata API'
            )
            connection_is_ok = False
        else:
            connection_is_ok = True
        if connection_is_ok:
            break
    if not response.ok:
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
