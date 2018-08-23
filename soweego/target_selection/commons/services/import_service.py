#!/usr/bin/env python3
# coding: utf-8

import threading

from soweego.target_selection.commons import constants as const
from ..utils import file_utils
from ..utils import http_client as client
from ..services import import_handlers as handlers
from ...domain.models.dump_state import DumpState

class ImportService(object):
    __dumps = []

    def __init__(self):
        __dumps = []


    def refresh_dump(self, dump_states: str, output: str) -> None:
        self.__load_states(dump_states)

        for dump in self.__dumps :
            last_modified = client.http_call(dump.location, 'HEAD').headers[const.last_modified_key]

            # checks if the current dump is up-to-date
            if dump.last_modified != last_modified :
                # TODO async 
                # thread = threading.Thread(target=self.__update_dump, args=(dump, last_modified))
                # thread.daemon = True
                # thread.start()        
                self.__update_dump(dump, last_modified, output)


    def __update_dump(self, dump, last_modified, output):
        file_path = '{0}/{1}'.format(output, dump.file_name)
        client.download_file(dump.location, file_path)
        try:
            handler = getattr(handlers, '{0}_handler'.format(dump.name)) 
            handler(file_path)
        except:
            pass # TODO use common logging system 
            #file_utils.log_error('Unable to scrape dump: {0}'.format(dump.name))
        dump.last_modified = last_modified
        self.__export_states()

    def __load_states(self, dump_states: str) -> None:
        try :
            serialized_dumps = file_utils.load_json(const.dump_states)
            for dictionary in serialized_dumps :
                self.__dumps.append(DumpState(dictionary["name"], dictionary["location"], dictionary["extension"], dictionary["base_uri"], dictionary["rdf_type"], dictionary["rdf_person"], dictionary["last_modified"]))
        except :
            pass # TODO use common logging system 
            # file_utils.log_error('Unable to parse .json file: {1}'.format(loc.dump_states)) 

    def __export_states(self):
        try :
            file_utils.export(const.dump_states, self.__dumps)
        except :
            pass # TODO use common logging system 
            # file_utils.log_error('Unable to export dump states') 
