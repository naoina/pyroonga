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
import logging

from pyroonga import utils

logger = logging.getLogger(__name__)


class QueryError(Exception):
    def __init__(self, msg):
        self.msg

    def __str__(self):
        return self.msg


class Query(object):
    def __init__(self, tbl):
        self._table = tbl


class SelectQuery(Query):
    def __init__(self, tbl, **kwargs):
        super(SelectQuery, self).__init__(tbl)
        self._target = kwargs

    def all(self):
        q = str(self)
        result = self._table.grn.query(q)
        return json.loads(result)

    def _makeparam(self):
        params = [utils.escape('%s:@%s' % target) for target in
                  self._target.items()]
        param = ' OR '.join(params)
        return param and '(' + param + ')'

    def __str__(self):
        return 'select --table "%s" --query "%s"' % (self._table.__tablename__,
                self._makeparam())
