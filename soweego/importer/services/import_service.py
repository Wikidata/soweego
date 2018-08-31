#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""

import logging
import os
import json

from soweego.importer import localizations as loc  
from soweego.importer import constants as const
from soweego.importer.utils import json_utils
from soweego.importer.utils import http_client as client
from soweego.importer.models.dump_state import DumpState
from soweego.importer.handlers import csv_handler as csv_handler
from soweego.importer.handlers import nt_handler as nt_handler


LOGGER = logging.getLogger(__name__)


class ImportService(object):
    """TODO docstring"""


    def refresh_dump(self, dump_state: DumpState, handler) -> DumpState:
        """TODO docstring"""

        last_modified = client.http_call(dump_state.download_url, 'HEAD').headers[const.LAST_MODIFIED_KEY]

        # checks if the current dump is up-to-date
        if dump_state.last_modified != last_modified or not os.path.isfile(dump_state.output_path):    
            try:
                self.__update_dump(dump_state, last_modified)
            except Exception as e:
                LOGGER.warning("%s\n%s", loc.FAIL_DOWNLOAD, str(e))
            else:
                try:
                    handler(dump_state.output_path)
                except Exception as e:
                    LOGGER.warning("%s\n%s", loc.FAIL_HANDLER, str(e))
         

    def __update_dump(self, dump: DumpState, last_modified: str) -> None:
        """TODO docstring"""
        client.download_file(dump.download_url, dump.output_path)
        dump.last_modified = last_modified