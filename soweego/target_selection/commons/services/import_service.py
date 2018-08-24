#!/usr/bin/env python3
# coding: utf-8

import threading
import logging

from soweego.target_selection.commons import constants as const
from ..utils import file_utils
from ..utils import http_client as client
from ..models.dump_state import DumpState

LOGGER = logging.getLogger(__name__)

class ImportService(object):
    __dumps = []

    def __init__(self):
        __dumps = []

    def refresh_dump(self, dump_states: str, output: str, handler) -> None:
        """TODO docstring"""
        self.__load_states(dump_states)

        for dump in self.__dumps :
            file_path = '{0}/{1}'.format(output, dump.file_name)
            last_modified = client.http_call(dump.location, 'HEAD').headers[const.last_modified_key]

            # checks if the current dump is up-to-date
            a = file_utils.is_empty(output)
            if dump.last_modified != last_modified or file_utils.is_empty(output):
                # TODO async 
                # thread = threading.Thread(target=self.__update_dump, args=(dump, last_modified, handler))
                # thread.daemon = True
                # thread.start()        
                self.__update_dump(dump, last_modified, file_path, handler)
            try:
                handler(file_path, output)
            except:
                LOGGER.warning('Unable to scrape dump: {0}'.format(dump.name))

    def __update_dump(self, dump: DumpState, last_modified: str, file_path: str, handler) -> None:
        """TODO docstring"""
        client.download_file(dump.location, file_path)
        dump.last_modified = last_modified
        self.__export_states()

    def __load_states(self, dump_states: str) -> None:
        """TODO docstring"""
        try :
            serialized_dumps = file_utils.load_json(const.dump_states)
            for dictionary in serialized_dumps :
                self.__dumps.append(DumpState(dictionary["name"], dictionary["location"], dictionary["extension"], dictionary["base_uri"], dictionary["rdf_type"], dictionary["rdf_person"], dictionary["last_modified"]))
        except :
            LOGGER.warning('Unable to parse .json file: {1}'.format(dump_states))

    def __export_states(self):
        """TODO docstring"""
        try :
            file_utils.export(const.dump_states, self.__dumps)
        except :
            LOGGER.warning('Unable to export dump states')
