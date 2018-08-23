#!/usr/bin/env python3
# coding: utf-8

import unittest
import business.utils.file_utils as file_utils
import domain.localizations as loc
from business.services.import_service import ImportService

class ImportTest(unittest.TestCase):

    def test_load_and_export_dump_states(self):
        # Arrange
        import_svc = ImportService()
        import_svc.load_states()
        file_utils.remove(loc.dump_states)
        
        # Act
        import_svc.export_states()

        # Assert
        self.assertTrue(file_utils.exists(loc.dump_states)) 

    def test_refresh(self):
        # Arrange
        # Act
        # Assert
        self.assertTrue(True) 