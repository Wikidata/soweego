#!/usr/bin/env python3
# coding: utf-8

import os
import sys
import json
import datetime
import codecs
from shutil import copyfile
from collections import namedtuple

from .. import constants as const

def json_deserialize(serialized_json: str) -> object:
    return json.loads(serialized_json, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

def load_json(source_file: str) -> object:
    try :
        with open(source_file) as file:
            return json.load(file)
    except :
        log_error('Unable to load .json file: {1}'.format(source_file)) 

def log_error(text: str):
    log('{0} \t {1} \n'.format(get_iso_time(), text), loc.log_file)

def log(text: str, file: str, access_method = 'ab+'):
    with codecs.open(file, access_method, "utf-8") as file:
        file.write('{0}\n'.format(text))

def exists(file: str) -> bool:
    return os.path.isfile(file)

def copy(source: str, destination: str) -> None:
    copyfile(source, destination)

def remove(source: str) -> None:
    os.remove(source)

def rename(old: str, new: str) -> None:
    os.rename(old, new)

def export(file: str, obj: object, mode = 'w') -> None:
    serialized_object = json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    log(serialized_object, file , mode)

def get_iso_time(time = datetime.datetime.now()) -> str:
    return '{0}Z/14'.format(time.replace(microsecond=0).isoformat())

def get_path(file: str) -> str:
    """Returns the path of the current resource folder"""
    path = os.path.abspath(file)
    return os.path.dirname(path)

def get_resource_path(file: str) -> str:
    """Returns the path of the current resource folder"""
    return get_folder_path(file, const.resource_folder)

def get_output_path(file: str) -> str:
    """Returns the path of the current output files"""
    return get_folder_path(file, const.output_folder)

def get_folder_path(file: str, folder: str) -> str:
    dir_path = '{0}/{1}'.format(get_path(file), folder)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path

def get_module_path() -> str:
    return os.path.dirname(sys.modules['__main__'].__file__)

def is_empty(folder: str) -> bool:
    return len(os.listdir(folder)) == 0

    