# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.


__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]

import json
import logging
import random
import string
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)


def sendquery(cmd):
    proc = Popen(['groonga', '-c'], stdin=PIPE, stdout=PIPE)
    result = proc.communicate(cmd.encode('utf-8'))[0]
    proc.wait()
    return result.decode('utf-8')


def insert(tablename, data):
    sendquery('load --table %s --input_type json --values\n%s' %
              (tablename, json.dumps(data)))


def random_string(n=20):
    return ''.join(random.choice(string.ascii_letters) for _ in range(n))


def gen_unique_tablename():
    name = random_string()
    while True:
        result = json.loads(sendquery('table_list'))
        names = [t[1] for t in result[1][1:]]
        if name not in names:
            return name


class classproperty(property):
    def __get__(self, inst, owner):
        return self.fget(owner)
