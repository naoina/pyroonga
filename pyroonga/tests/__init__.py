# -*- coding: utf-8 -*-

import json
import os
import shutil

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from subprocess import Popen, PIPE
from signal import SIGTERM
from multiprocessing import Process, Pipe

import pyroonga


class GroongaTestBase(unittest.TestCase):
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixture')
    FIXTURE_PATH = os.path.join(FIXTURE_DIR, 'dbfixture%s.json')
    DB_DIR = os.path.join(FIXTURE_DIR, 'db')
    DB_PATH = os.path.join(DB_DIR, 'test.db')

    @classmethod
    def setUpClass(cls):
        parent_conn, child_conn = Pipe()
        proc = Process(target=cls._start_groonga, args=(child_conn,))
        proc.daemon = True
        proc.start()
        cls.serverpid = parent_conn.recv()
        proc.join(1)
        # 生存戦略
        if not proc.is_alive():
            raise RuntimeError(parent_conn.recv())

    @classmethod
    def tearDownClass(cls):
        cls._stop_groonga()
        cls._remove_dbdir()

    @classmethod
    def _start_groonga(cls, conn):
        if os.path.isdir(cls.DB_DIR):
            cls._remove_dbdir()
        os.makedirs(cls.DB_DIR)
        Popen('groonga -n %s quit' % cls.DB_PATH, shell=True, stdout=PIPE,
              stderr=PIPE).wait()
        server = Popen('groonga -s ' + cls.DB_PATH, shell=True, stdout=PIPE,
                       stderr=PIPE)
        conn.send(server.pid)
        server.wait()
        conn.send(server.stderr.read().decode('utf-8'))

    @classmethod
    def _stop_groonga(cls):
        try:
            os.kill(cls.serverpid, SIGTERM)
        except OSError:
            pass  # ignore

    @classmethod
    def _remove_dbdir(cls):
        if os.path.isdir(cls.DB_DIR):
            shutil.rmtree(cls.DB_DIR)

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
