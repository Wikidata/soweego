#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dump extractor abstract class"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import Iterable


class BaseDumpExtractor:

    def extract_and_populate(self, dump_file_path: Iterable[str], resolve: bool):
        """Extract relevant data and populate SQL Alchemy entities accordingly.

        :param dump_file_path: Iterable of paths where downloaded dumps are placed.
        :param resolve: Tells if the system will resolve the urls to validate them
        :raises NotImplementedError: you have to override this method
        """
        raise NotImplementedError


    def get_dump_download_urls(self) -> Iterable[str]:
        """Get the dump download URL.
        Useful if there is a way to compute the latest dump URL.

        :raises NotImplementedError: overriding this method is optional
        :return: the latest dump URL
        :rtype: str
        """
        raise NotImplementedError

    def _commit_entity(self, db_manager, entity):
        success = True
        session = db_manager.new_session()
        try:
            session.add(entity)
            session.commit()
        except:
            session.rollback()
            success = False
        finally:
            session.close()

        if not success:
            self._commit_entity(db_manager, entity)
