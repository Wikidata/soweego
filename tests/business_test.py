#!/usr/bin/env python3
# coding: utf-8

import unittest


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