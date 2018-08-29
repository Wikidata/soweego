#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

import logging
import os

from soweego.importer import constants as const
from soweego.importer.utils import json_utils
from soweego.importer.utils import http_client as client
from soweego.importer.models.dump_state import DumpState

LOGGER = logging.getLogger(__name__)

class ImportService(object):
    __dumps = []

    def __init__(self):
        __dumps = []

    def refresh_dumps(self, dump_states: str, output: str) -> None:
        """TODO docstring"""
        self.__load_states(dump_states)

        for dump in self.__dumps:
            file_path = '{0}/{1}'.format(output, dump.file_name)
            last_modified = client.http_call(dump.location, 'HEAD').headers[const.LAST_MODIFIED_KEY]

            # checks if the current dump is up-to-date
            if dump.last_modified != last_modified or len(os.listdir(output)) == 0:
                # TODO async 
                # thread = threading.Thread(target=self.__update_dump, args=(dump, last_modified, handler))
                # thread.daemon = True
                # thread.start()        
                self.__update_dump(dump, last_modified, file_path)
            try:
                handler(file_path, output)
            except:
                LOGGER.warning('Unable to scrape dump: {0}'.format(dump.name))

    def refresh_dump(self, dump_state: str, output: str) -> None:
        """TODO docstring"""
        self.__load_states(dump_states)

        for dump in self.__dumps:
            file_path = '{0}/{1}'.format(output, dump.file_name)
            last_modified = client.http_call(dump.location, 'HEAD').headers[const.LAST_MODIFIED_KEY]

            # checks if the current dump is up-to-date
            if dump.last_modified != last_modified or len(os.listdir(output)) == 0:
                # TODO async 
                # thread = threading.Thread(target=self.__update_dump, args=(dump, last_modified, handler))
                # thread.daemon = True
                # thread.start()        
                self.__update_dump(dump, last_modified, file_path)
            try:
                handler(file_path, output)
            except:
                LOGGER.warning('Unable to scrape dump: {0}'.format(dump.name))

    def __update_dump(self, dump: DumpState, last_modified: str, file_path: str) -> None:
        """TODO docstring"""
        client.download_file(dump.location, file_path)
        dump.last_modified = last_modified
        self.__export_states()

    def __load_states(self, dump_states: str) -> None:
        """TODO docstring"""
        try:
            serialized_dumps = json_utils.load(const.DUMP_STATES)
            for dictionary in serialized_dumps :
                self.__dumps.append(DumpState(dictionary["name"], dictionary["location"], dictionary["extension"], dictionary["base_uri"], dictionary["rdf_type"], dictionary["rdf_person"], dictionary["last_modified"]))
        except:
            LOGGER.warning('Unable to parse .json file: {1}'.format(dump_states))

    def __export_states(self):
        """TODO docstring"""
        try:
            json_utils.export(const.DUMP_STATES, self.__dumps)
        except:
            LOGGER.warning('Unable to export dump states')
