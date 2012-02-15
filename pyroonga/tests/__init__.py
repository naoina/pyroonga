# -*- coding: utf-8 -*-

# Copyright (c) 2012 Naoya INADA <naoina@kuune.org>
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.


__author__ = "Naoya INADA <naoina@kuune.org>"

__all__ = [
]

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

    def loadfixture(self, num):
        return json.load(open(self.FIXTURE_PATH % num))


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
