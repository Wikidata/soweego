#!/usr/bin/env python3
# coding: utf-8

import datetime
import re

from ..utils import file_utils
from ...domain.models.bibsys_model import Bibsys

def bibsys_handler(file_path: str, output_dir: str):
    if not file_utils.exists(file_path):
        raise Exception("file: {0} not found".format(file_path))
    with open(file_path) as file:
        rows = file.readlines() 

        # this method requires at least twice the size of the dump in memory (~2.4GB)
        # TODO async

        # assume that every id has a fixed length of 8 digits
        dictionary = {}

        # TODO log
        # print ('{0} \t Start import'.format(datetime.datetime.now()))
        
        counter = 0
        for row in rows:
            counter+=1
            if(counter > 10000):
                break
            try :
                regex = re.compile('x[0-9]+')
                current_id = int(regex.search(row).group(0).replace('x', ''))

                if row.find('Person') != -1 :
                    if current_id in dictionary:
                        dictionary[current_id].is_person = True 
                    else :
                        dictionary[current_id] = Bibsys(identifier = current_id, is_person = True)

                if row.find('name') != -1:
                    regex = re.compile('".*"')
                    name = decode_name(regex.search(row).group(0).replace('"', ''))
                    if current_id in dictionary:
                        dictionary[current_id].name = name 
                    else :
                        dictionary[current_id] = Bibsys(identifier = current_id, name = name)

                if row.find('since') != -1:
                    regex = re.compile('".*"')
                    since = regex.search(row).group(0).replace('"', '')
                    if current_id in dictionary:
                        dictionary[current_id].since = since  
                    else :
                        dictionary[current_id] = Bibsys(identifier = current_id, since = since)

                if row.find('until') != -1:
                    regex = re.compile('".*"')
                    until = regex.search(row).group(0).replace('"', '')
                    if current_id in dictionary:
                        dictionary[current_id].until = until  
                    else :
                        dictionary[current_id] = Bibsys(identifier = current_id, until = until)

                if row.find('sameAs') != -1:
                    regex = re.compile('<.*>')
                    same_as = regex.search(row).group(0).replace('<', '').replace('>', '').split(' ')[2]
                    if current_id in dictionary:
                        dictionary[current_id].same_as.append(same_as)  
                    else :
                        dictionary[current_id] = Bibsys(identifier = current_id, same_as = [same_as])

            except : 
                pass # TODO log
                # file_utils.log_error("Error at row: {0}".format(row))

    print ('{0} \t End import'.format(datetime.datetime.now()))
    file_utils.export(loc.bibsys_dict, dictionary)

def decode_name(name:str) -> str :
    return name.encode('ascii').decode('unicode-escape') 