#!/usr/bin/env python3
# coding: utf-8

"""Baseline helper for dump scraping"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import datetime
import re
import logging
import os

from soweego.commons.json_utils import export
from soweego.target_selection.commons import constants
from .models.bibsys_metadata import BibsysMetadata
from soweego.commons.file_utils import get_path

LOGGER = logging.getLogger(__name__)


def name_scraper(file_path: str):
    if not os.path.isfile(file_path):
        raise Exception("file: {0} not found".format(file_path))
    with open(file_path) as file:
        rows = file.readlines() 

        dictionary = {}

        LOGGER.info('{0} \t Start bibsys import'.format(datetime.datetime.now()))
        
        counter = 0
        for row in rows:
            if(counter > 10000):
                break
            try:
                regex = re.compile('x[0-9]+')
                current_id = int(regex.search(row).group(0).replace('x', ''))

                if row.find('Person') != -1 :
                    if current_id in dictionary:
                        dictionary[current_id].is_person = True 
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, is_person = True)

                if row.find('name') != -1:
                    name = find(row, "\"", "\"")
                    if current_id in dictionary:
                        dictionary[current_id].name = name 
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, name = name)

            except Exception as exception: 
                LOGGER.warning('Error at row: %s. \n %s', row, str(exception))
    LOGGER.info('%s \t End bibsys name scraping', datetime.datetime.now())
    export(('%s\\%s' % output, constants.BIBSYS_DICTIONARY), dictionary)


def bibsys_scraper(file_path: str, output: str):
    if not os.path.isfile(file_path):
        raise Exception("file: {0} not found".format(file_path))
    with open(file_path) as file:
        rows = file.readlines() 

        # this method requires at least twice the size of the dump in memory (~2.4GB)
        # TODO async

        # assume that every id has a fixed length of 8 digits
        dictionary = {}

        LOGGER.info('{0} \t Start bibsys import'.format(datetime.datetime.now()))
        
        counter = 0
        for row in rows:
            counter+=1
            if(counter > 10000):
                break
            try:
                regex = re.compile('x[0-9]+')
                current_id = int(regex.search(row).group(0).replace('x', ''))

                if row.find('Person') != -1 :
                    if current_id in dictionary:
                        dictionary[current_id].is_person = True 
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, is_person = True)

                if row.find('name') != -1:
                    name = find(row, "\"", "\"")
                    if current_id in dictionary:
                        dictionary[current_id].name = name 
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, name = name)

                if row.find('since') != -1:
                    since = find(row, "\"", "\"")
                    if current_id in dictionary:
                        dictionary[current_id].since = since  
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, since = since)

                if row.find('until') != -1:
                    until = find(row, "\"", "\"")
                    if current_id in dictionary:
                        dictionary[current_id].until = until  
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, until = until)

                if row.find('sameAs') != -1:
                    same_as = find(row, "<", ">")
                    if current_id in dictionary:
                        dictionary[current_id].same_as.append(same_as)  
                    else :
                        dictionary[current_id] = BibsysMetadata(identifier = current_id, same_as = [same_as])

            except Exception as exception: 
                LOGGER.warning('Error at row: %s. \n %s', row, str(exception))
    LOGGER.info('%s \t End bibsys import', datetime.datetime.now())
    export(('%s\\%s' % output, constants.BIBSYS_DICTIONARY), dictionary)

def find(row: str, left_delimiter: str, right_delimiter: str) -> str:
    r = '%s.*%s' % (left_delimiter, right_delimiter)
    regex = re.compile(r)
    return regex.search(row).group(0).replace(left_delimiter, '').replace(right_delimiter, '').split(' ')[2]


def decode_name(name: str) -> str:
    return name.encode('ascii').decode('unicode-escape') 
