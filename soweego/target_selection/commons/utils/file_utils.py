#!/usr/bin/env python3
# coding: utf-8

import os
import json
import datetime
import codecs
from shutil import copyfile
from collections import namedtuple

from .. import constants as const

def json_deserialize(serialized_json):
    return json.loads(serialized_json, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

def load_json(source_file):
    try :
        with open(source_file) as file:
            return json.load(file)
    except :
        log_error('Unable to load .json file: {1}'.format(source_file)) 

def log_error(text):
    log('{0} \t {1} \n'.format(get_iso_time(), text), loc.log_file)

def log(text, file, access_method = 'ab+'):
    with codecs.open(file, access_method, "utf-8") as file:
        file.write('{0}\n'.format(text))

def exists(file):
    return os.path.isfile(file)

def copy(source, destination):
    copyfile(source, destination)

def remove(source):
    os.remove(source)

def rename(old, new):
    os.rename(old, new)

def export(file, obj, mode = 'w'):
    serialized_object = json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    log(serialized_object, file , mode)

def get_iso_time(time = datetime.datetime.now()):
    return '{0}Z/14'.format(time.replace(microsecond=0).isoformat())

def get_path(file):
    """Returns the path of the current resource folder"""
    path = os.path.abspath(file)
    return os.path.dirname(path)

def get_resource_path(file):
    """Returns the path of the current resource folder"""
    return get_folder_path(file, const.resource_folder)

def get_output_path(file):
    """Returns the path of the current output files"""
    return get_folder_path(file, const.output_folder)

def get_folder_path(file, folder):
    dir_path = '{0}/{1}'.format(get_path(file), folder)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path