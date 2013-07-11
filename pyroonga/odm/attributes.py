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
    'Symbol', 'TableFlags', 'ColumnFlagsFlag', 'ColumnFlags', 'DataType',
    'TokenizerSymbol', 'Tokenizer', 'LogLevel', 'SuggestType',
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


class TableFlagsFlag(Flags):
    """Table flags representation class of groonga"""


class TableFlags(object):
    "Table flags"

    TABLE_HASH_KEY = TableFlagsFlag(Symbol('TABLE_HASH_KEY'))
    TABLE_PAT_KEY  = TableFlagsFlag(Symbol('TABLE_PAT_KEY'))
    TABLE_DAT_KEY  = TableFlagsFlag(Symbol('TABLE_DAT_KEY'))
    TABLE_NO_KEY   = TableFlagsFlag(Symbol('TABLE_NO_KEY'))
    TABLE_VIEW     = TableFlagsFlag(Symbol('TABLE_VIEW'))
    KEY_WITH_SIS   = TableFlagsFlag(Symbol('KEY_WITH_SIS'))
    KEY_NORMALIZE  = TableFlagsFlag(Symbol('KEY_NORMALIZE'))
    PERSISTENT     = TableFlagsFlag(Symbol('PERSISTENT'))


class ColumnFlagsFlag(Flags):
    """Column flags representation class of groonga"""


class ColumnFlags(object):
    "Column flags"

    COLUMN_SCALAR = ColumnFlagsFlag(Symbol('COLUMN_SCALAR'))
    COLUMN_VECTOR = ColumnFlagsFlag(Symbol('COLUMN_VECTOR'))
    COLUMN_INDEX  = ColumnFlagsFlag(Symbol('COLUMN_INDEX'))
    WITH_SECTION  = ColumnFlagsFlag(Symbol('WITH_SECTION'))
    WITH_WEIGHT   = ColumnFlagsFlag(Symbol('WITH_WEIGHT'))
    WITH_POSITION = ColumnFlagsFlag(Symbol('WITH_POSITION'))
    RING_BUFFER   = ColumnFlagsFlag(Symbol('RING_BUFFER'))


class DataTypeSymbol(Symbol):
    """Data type representation class of groonga"""


class DataType(object):
    """Builtin types"""

    Object    = DataTypeSymbol('Object')
    Bool      = DataTypeSymbol('Bool')
    Int8      = DataTypeSymbol('Int8')
    UInt8     = DataTypeSymbol('UInt8')
    Int16     = DataTypeSymbol('Int16')
    UInt16    = DataTypeSymbol('UInt16')
    Int32     = DataTypeSymbol('Int32')
    UInt32    = DataTypeSymbol('UInt32')
    Int64     = DataTypeSymbol('Int64')
    UInt64    = DataTypeSymbol('UInt64')
    Float     = DataTypeSymbol('Float')
    Time      = DataTypeSymbol('Time')
    ShortText = DataTypeSymbol('ShortText')
    Text      = DataTypeSymbol('Text')
    LongText  = DataTypeSymbol('LongText')
    TokyoGeoPoint = DataTypeSymbol('TokyoGeoPoint')
    WGS84GeoPoint = DataTypeSymbol('WGS84GeoPoint')


class TokenizerSymbol(Symbol):
    """Tokenizer type representation class of groonga"""


class Tokenizer(object):
    """Tokenizer"""

    TokenDelimit = TokenizerSymbol('TokenDelimit')
    TokenUnigram = TokenizerSymbol('TokenUnigram')
    TokenBigram  = TokenizerSymbol('TokenBigram')
    TokenTrigram = TokenizerSymbol('TokenTrigram')
    TokenMecab   = TokenizerSymbol('TokenMecab')


class LogLevelSymbol(Symbol):
    """Log lovel representation class of groonga"""


class LogLevel(object):
    """Log level"""

    EMERG     = LogLevelSymbol('EMERG')
    ALERT     = LogLevelSymbol('ALERT')
    CRIT      = LogLevelSymbol('CRIT')
    error     = LogLevelSymbol('error')
    warning   = LogLevelSymbol('warning')
    notice    = LogLevelSymbol('notice')
    info      = LogLevelSymbol('info')
    debug     = LogLevelSymbol('debug')
    EMERGENCY = EMERG   # alias
    CRITICAL  = CRIT    # alias
    WARNING   = warning # alias
    NOTICE    = notice  # alias
    INFO      = info    # alias
    DEBUG     = debug   # alias


class SuggestTypeFlag(Flags):
    """Type flags representation class of groonga's suggest"""


class SuggestType(object):
    """Suggest types"""

    complete = SuggestTypeFlag(Symbol('complete'))
    correct  = SuggestTypeFlag(Symbol('correct'))
    suggest  = SuggestTypeFlag(Symbol('suggest'))
    COMPLETE = complete  # alias
    CORRECT  = correct   # alias
    SUGGEST  = suggest   # alias
