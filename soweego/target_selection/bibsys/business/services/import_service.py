#!/usr/bin/env python3
# coding: utf-8

import threading
import domain.localizations as loc
import business.utils.file_utils as file_utils
import business.utils.http_client as client
import business.services.import_handlers as Handlers 
from domain.models.dump_state import DumpState

class ImportService(object):
    __dumps = []

    def __init__(self):
        __dumps = []

    def refresh(self):
        self.load_states()

        for dump in self.__dumps :
            last_modified = client.http_call(dump.location, 'HEAD').headers[loc.last_modified_key]

            # checks if the current dump is up-to-date
            if dump.last_modified != last_modified :
                # thread = threading.Thread(target=self.update, args=(dump, last_modified))
                # thread.daemon = True
                # thread.start()        
                self.update(dump, last_modified)

    def update(self, dump, last_modified):
        #client.download_file(dump.location, '{0}/{1}'.format(loc.temporary_directory, dump.file_name))
        #try:
        handler = getattr(Handlers, '{0}_handler'.format(dump.name)) 
        #handler = getattr(Handlers, '{0}_schema'.format(dump.name)) 
        handler(dump)
        #except:
            #file_utils.log_error('Unable to scrape dump: {0}'.format(dump.name))
        #dump.last_modified = last_modified
        self.export_states()

    def load_states(self):
        try :
            serialized_dumps = file_utils.load_json(loc.dump_states)
            for dictionary in serialized_dumps :
                self.__dumps.append(DumpState(dictionary["name"], dictionary["location"], dictionary["extension"], dictionary["base_uri"], dictionary["rdf_type"], dictionary["rdf_person"], dictionary["last_modified"]))
        except :
            file_utils.log_error('Unable to parse .json file: {1}'.format(loc.dump_states)) 

    def export_states(self):
        try :
            file_utils.export(loc.dump_states, self.__dumps)
        except :
            file_utils.log_error('Unable to export dump states') 
