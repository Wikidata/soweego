#!/usr/bin/python

import os, json, datetime
import domain.localizations as loc 
from shutil import copyfile

BACKUP_EXTENSION = 'backup'

def load_json(source_file):
    try :
        with open(source_file) as file:
            return json.load(file)
    except :
        log_error('Unable to load .json file: {1}'.format(source_file)) 

def log_error(text):
    log('{0} \t {1} \n'.format(get_iso_time(), text), loc.log_file)

def log(text, file, access_method = 'ab+'):
    with open(file, access_method) as f:
        f.write('{0}\n'.format(text))

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