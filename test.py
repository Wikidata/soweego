#!/usr/bin/python

import tests.business_test as business_test
import unittest

def run_test(test_suite) :
    suite = unittest.TestLoader().loadTestsFromTestCase(test_suite)
    unittest.TextTestRunner(verbosity=2).run(suite)

run_test(business_test.ImportTest)