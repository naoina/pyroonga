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


"""Attributes of groonga"""

__author__ = "Naoya INADA <naoina@kuune.org>"

__all__ = [
    'Symbol', 'TableFlags', 'TABLE_HASH_KEY', 'TABLE_PAT_KEY', 'TABLE_DAT_KEY',
    'TABLE_NO_KEY', 'TABLE_VIEW', 'KEY_WITH_SIS', 'KEY_NORMALIZE',
    'PERSISTENT', 'ColumnFlags', 'COLUMN_SCALAR', 'COLUMN_VECTOR',
    'COLUMN_INDEX', 'WITH_SECTION', 'WITH_WEIGHT', 'WITH_POSITION', 'DataType',
    'Object', 'Bool', 'Int8', 'UInt8', 'Int16', 'UInt16', 'Int32', 'UInt32',
    'Int64', 'UInt64', 'Float', 'Time', 'ShortText', 'Text', 'LongText',
    'TokyoGeoPoint', 'WGS84GeoPoint', 'Tokenizer', 'TokenDelimit',
    'TokenUnigram', 'TokenBigram', 'TokenTrigram', 'TokenMecab', 'LogLevel',
    'EMERG', 'ALERT', 'CRIT', 'error', 'warning', 'notice', 'info', 'debug',
    'EMERGENCY', 'CRITICAL', 'WARNING', 'NOTICE', 'INFO', 'DEBUG',
]

from collections import Iterable


class Symbol(object):
    """Symbol representation class of groonga"""

    __slots__ = ['name']

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


class Flags(list):
    """Flags representation class of groonga"""

    __slots__ = []

    def __init__(self, flags):
        if not isinstance(flags, str) and isinstance(flags, Iterable):
            self.extend(flags)
        elif isinstance(flags, Symbol):
            self.append(flags)
        else:
            raise TypeError('"flags" type must be instance of Symbol or list')

    def __and__(self, other):
        cls = self.__class__
        if not isinstance(other, cls):
            raise TypeError('operand type must be instance of %s' %
                    cls.__name__)
        for item in other:
            if item not in self:
                return False
        return True

    def __or__(self, other):
        cls = self.__class__
        if not isinstance(other, cls):
            raise TypeError('operand type must be instance of %s' %
                    cls.__name__)
        return cls(self + other)

    def __str__(self):
        return '|'.join([str(flag) for flag in self])


class TableFlags(Flags):
    """Table flags representation class of groonga"""

TABLE_HASH_KEY = TableFlags(Symbol('TABLE_HASH_KEY'))
TABLE_PAT_KEY  = TableFlags(Symbol('TABLE_PAT_KEY'))
TABLE_DAT_KEY  = TableFlags(Symbol('TABLE_DAT_KEY'))
TABLE_NO_KEY   = TableFlags(Symbol('TABLE_NO_KEY'))
TABLE_VIEW     = TableFlags(Symbol('TABLE_VIEW'))
KEY_WITH_SIS   = TableFlags(Symbol('KEY_WITH_SIS'))
KEY_NORMALIZE  = TableFlags(Symbol('KEY_NORMALIZE'))
PERSISTENT     = TableFlags(Symbol('PERSISTENT'))


class ColumnFlags(Flags):
    """Column flags representation class of groonga"""

COLUMN_SCALAR = ColumnFlags(Symbol('COLUMN_SCALAR'))
COLUMN_VECTOR = ColumnFlags(Symbol('COLUMN_VECTOR'))
COLUMN_INDEX  = ColumnFlags(Symbol('COLUMN_INDEX'))
WITH_SECTION  = ColumnFlags(Symbol('WITH_SECTION'))
WITH_WEIGHT   = ColumnFlags(Symbol('WITH_WEIGHT'))
WITH_POSITION = ColumnFlags(Symbol('WITH_POSITION'))


class DataType(Symbol):
    """Data type representation class of groonga"""

# builtin types
Object    = DataType('Object')
Bool      = DataType('Bool')
Int8      = DataType('Int8')
UInt8     = DataType('UInt8')
Int16     = DataType('Int16')
UInt16    = DataType('UInt16')
Int32     = DataType('Int32')
UInt32    = DataType('UInt32')
Int64     = DataType('Int64')
UInt64    = DataType('UInt64')
Float     = DataType('Float')
Time      = DataType('Time')
ShortText = DataType('ShortText')
Text      = DataType('Text')
LongText  = DataType('LongText')
TokyoGeoPoint = DataType('TokyoGeoPoint')
WGS84GeoPoint = DataType('WGS84GeoPoint')


# tokenizer
class Tokenizer(Symbol):
    """Tokenizer type representation class of groonga"""

TokenDelimit = Tokenizer('TokenDelimit')
TokenUnigram = Tokenizer('TokenUnigram')
TokenBigram  = Tokenizer('TokenBigram')
TokenTrigram = Tokenizer('TokenTrigram')
TokenMecab   = Tokenizer('TokenMecab')


# log level
class LogLevel(Symbol):
    """Log lovel representation class of groonga"""

EMERG     = LogLevel('EMERG')
ALERT     = LogLevel('ALERT')
CRIT      = LogLevel('CRIT')
error     = LogLevel('error')
warning   = LogLevel('warning')
notice    = LogLevel('notice')
info      = LogLevel('info')
debug     = LogLevel('debug')

# alias
EMERGENCY = EMERG
CRITICAL  = CRIT
WARNING   = warning
NOTICE    = notice
INFO      = info
DEBUG     = debug
