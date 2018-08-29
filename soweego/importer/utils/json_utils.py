#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""utility for .json file management"""

import logging
import json
import codecs
from collections import namedtuple


LOGGER = logging.getLogger(__name__)


def deserialize(serialized_json: str) -> object:
    """json.loads wrapper, converts the result dictionary into an object"""
    return json.loads(
        serialized_json, 
        object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))


def load(source_file: str) -> object:
    """json.load wrapper, logs eventuals warning"""
    try:
        with open(source_file) as file:
            return json.load(file)
    except Exception as exception:
        LOGGER.warning("""Unable to load .json file: %s.\n %s""",
                       source_file, str(exception)) 

def export(file_path: str, obj: object, mode = 'w') -> None:
    """serializes an object and exports it to a file"""
    serialized_object = json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    with codecs.open(file_path, mode, 'utf-8') as file:
        file.write('%s\n', serialized_object)
