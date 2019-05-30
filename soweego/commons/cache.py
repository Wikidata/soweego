#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Adapted from https://github.com/Wikidata/StrepHit/blob/master/strephit/commons/cache.py

"""Caching facility"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import hashlib
import json
import os
import tempfile

BASE_DIR = os.path.join(tempfile.gettempdir(), 'soweego_cache')
ENABLED = True


def _hash_for(key):
    return hashlib.sha1(key.encode('utf8')).hexdigest()


def _path_for(hashed_key):
    """ Computes the path in which the given key should be stored.

        :return: tuple (full path, base path, file name)
        :rtype: tuple
    """
    loc = os.path.join(BASE_DIR, hashed_key[:3])
    return os.path.join(loc, hashed_key), loc, hashed_key


def get_value(key, default=None):
    """ Retrieves an item from the cache

        :param key: Key of the item
        :param default: Default value to return if the
         key is not in the cache
        :return: The item associated with the given key or
         the default value

        Sample usage:

        >>> from soweego.commons import cache
        >>> cache.get_value('kk', 13)
        13
        >>> cache.get_value('kk', 0)
        0
        >>> cache.set_value('kk', 15)
        >>> cache.get_value('kk', 0)
        15
    """
    if not ENABLED:
        return default

    hashed = _hash_for(key)
    loc, _, _ = _path_for(hashed)
    if os.path.exists(loc):
        with open(loc) as f:
            stored_key = f.readline()[:-1]
            if stored_key == key:
                return json.loads(f.read())
            return get_value(key + hashed, default)
    else:
        return default


def set_value(key, value, overwrite=True):
    """ Stores an item in the cache under the given key

        :param key: Unique key used to identify the idem.
        :param value: Value to store in the cache. Must be
        a generator of JSON-dumpable objects
        :param overwrite: Whether to overwrite the previous
        value associated with the key (if any)
        :return: Nothing

        Sample usage:

        >>> from soweego.commons import cache
        >>> cache.get_value('kk', 13)
        13
        >>> cache.get_value('kk', 0)
        0
        >>> cache.set_value('kk', 15)
        >>> cache.get_value('kk', 0)
        15
    """
    if not ENABLED:
        return

    hashed = _hash_for(key)
    loc, path, _ = _path_for(hashed)
    if not os.path.exists(loc):
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError:
                pass

        with open(loc, 'w') as f:
            f.write(key + '\n')
            f.write(json.dumps([obj for obj in value]))
    else:
        with open(loc, 'r+') as f:
            stored_key = f.readline()[:-1]
            if stored_key == key:
                if overwrite:
                    f.write(json.dumps([obj for obj in value]))
                    f.truncate()
                return
        set_value(key + hashed, value, overwrite)


def cached(function):
    """ Decorator to cache function results based on its arguments

    Sample usage:

    >>> from soweego.commons.cache import cached
    >>> @cached
    ... def f(x):
    ...     print('inside f')
    ...     return 2 * x
    ...
    >>> f(10)
    inside f
    20
    >>> f(10)
    20

    """

    def wrapper(*args, **kwargs):
        key = (
            str([function.__module__])
            + function.__name__
            + str(args)
            + str(kwargs)
        )
        res = get_value(key)
        if res is None:
            res = function(*args, **kwargs)
            if res is not None:
                set_value(key, res)
        return res

    return wrapper
