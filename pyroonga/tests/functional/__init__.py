# -*- coding: utf-8 -*-

import json
import os

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pyroonga


class GroongaTestBase(object):
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixture')
    FIXTURE_PATH = os.path.join(FIXTURE_DIR, 'dbfixture%s.json')

    def loadfixture(self, suffix):
        return json.load(open(self.FIXTURE_PATH % suffix))


class TestGroonga(GroongaTestBase):
    def test_connect(self):
        grn = pyroonga.connect()
        self.assertTrue(grn.connected)
        self.assertEqual(grn.host, '0.0.0.0')
        self.assertEqual(grn.port, 10041)

        grn = pyroonga.connect(host='localhost', port=10041)
        self.assertTrue(grn.connected)
        self.assertEqual(grn.host, 'localhost')
        self.assertEqual(grn.port, 10041)

        self.assertRaises(pyroonga.GroongaError, pyroonga.connect,
                host='unknown', port=10041)
        self.assertRaises(pyroonga.GroongaError, pyroonga.connect,
                host='localhost', port=1)


def main():
    unittest.main()

if __name__ == '__main__':
    main()
