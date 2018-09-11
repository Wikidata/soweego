#!/usr/bin/env python3
# coding: utf-8

"""Generic service for dump updating/importing"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import logging
import os

from soweego.commons import constants as const
from soweego.commons import http_client as client
from soweego.commons import localizations as loc
from soweego.commons.json_utils import export
from soweego.importer.commons.models.dump_state import DumpState

LOGGER = logging.getLogger(__name__)


class ImportService(object):

    def refresh_dump(self, dump_state_path, dump_state: DumpState, handler) -> DumpState:
        """Downloads the dump, if necessary, 
        and calls the handler over the dump file"""

        last_modified = client.http_call(dump_state.download_url,
                                         'HEAD').headers[const.LAST_MODIFIED_KEY]

        # checks if the current dump is up-to-date
        if dump_state.last_modified != last_modified or not os.path.isfile(dump_state.output_path):
            try:
                self.__update_dump(dump_state, last_modified)
            except Exception as e:
                LOGGER.warning("%s\n%s", loc.FAIL_DOWNLOAD, str(e))
        export(dump_state_path, dump_state)
        try:
            handler(dump_state.output_path)
        except Exception as e:
            LOGGER.warning("%s\n%s", loc.FAIL_HANDLER, str(e))

    def __update_dump(self, dump: DumpState, last_modified: str) -> None:
        """Downloads the dump"""
        client.download_file(dump.download_url, dump.output_path)
        dump.last_modified = last_modified
