# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

"""Attributes of groonga"""

__author__ = "Naoya Inada <naoina@kuune.org>"

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
    TABLE_PAT_KEY = TableFlagsFlag(Symbol('TABLE_PAT_KEY'))
    TABLE_DAT_KEY = TableFlagsFlag(Symbol('TABLE_DAT_KEY'))
    TABLE_NO_KEY = TableFlagsFlag(Symbol('TABLE_NO_KEY'))
    KEY_WITH_SIS = TableFlagsFlag(Symbol('KEY_WITH_SIS'))
    PERSISTENT = TableFlagsFlag(Symbol('PERSISTENT'))


class ColumnFlagsFlag(Flags):
    """Column flags representation class of groonga"""


class ColumnFlags(object):
    "Column flags"

    COLUMN_SCALAR = ColumnFlagsFlag(Symbol('COLUMN_SCALAR'))
    COLUMN_VECTOR = ColumnFlagsFlag(Symbol('COLUMN_VECTOR'))
    COLUMN_INDEX = ColumnFlagsFlag(Symbol('COLUMN_INDEX'))
    WITH_SECTION = ColumnFlagsFlag(Symbol('WITH_SECTION'))
    WITH_WEIGHT = ColumnFlagsFlag(Symbol('WITH_WEIGHT'))
    WITH_POSITION = ColumnFlagsFlag(Symbol('WITH_POSITION'))
    RING_BUFFER = ColumnFlagsFlag(Symbol('RING_BUFFER'))


class DataTypeSymbol(Symbol):
    """Data type representation class of groonga"""


class DataType(object):
    """Builtin types"""

    Object = DataTypeSymbol('Object')
    Bool = DataTypeSymbol('Bool')
    Int8 = DataTypeSymbol('Int8')
    UInt8 = DataTypeSymbol('UInt8')
    Int16 = DataTypeSymbol('Int16')
    UInt16 = DataTypeSymbol('UInt16')
    Int32 = DataTypeSymbol('Int32')
    UInt32 = DataTypeSymbol('UInt32')
    Int64 = DataTypeSymbol('Int64')
    UInt64 = DataTypeSymbol('UInt64')
    Float = DataTypeSymbol('Float')
    Time = DataTypeSymbol('Time')
    ShortText = DataTypeSymbol('ShortText')
    Text = DataTypeSymbol('Text')
    LongText = DataTypeSymbol('LongText')
    TokyoGeoPoint = DataTypeSymbol('TokyoGeoPoint')
    WGS84GeoPoint = DataTypeSymbol('WGS84GeoPoint')


class TokenizerSymbol(Symbol):
    """Tokenizer type representation class of groonga"""


class Tokenizer(object):
    """Tokenizer"""

    TokenBigram = TokenizerSymbol('TokenBigram')
    TokenBigramSplitSymbol = TokenizerSymbol('TokenBigramSplitSymbol')
    TokenBigramSplitSymbolAlpha = TokenizerSymbol('TokenBigramSplitSymbolAlpha')
    TokenBigramSplitSymbolAlphaDigit = TokenizerSymbol('TokenBigramSplitSymbolAlphaDigit')
    TokenBigramIgnoreBlank = TokenizerSymbol('TokenBigramIgnoreBlank')
    TokenBigramIgnoreBlankSplitSymbol = TokenizerSymbol('TokenBigramIgnoreBlankSplitSymbol')
    TokenBigramIgnoreBlankSplitAlpha = TokenizerSymbol('TokenBigramIgnoreBlankSplitAlpha')
    TokenBigramIgnoreBlankSplitAlphaDigit = TokenizerSymbol('TokenBigramIgnoreBlankSplitAlphaDigit')
    TokenDelimit = TokenizerSymbol('TokenDelimit')
    TokenDelimitNull = TokenizerSymbol('TokenDelimitNull')
    TokenTrigram = TokenizerSymbol('TokenTrigram')
    TokenUnigram = TokenizerSymbol('TokenUnigram')


class NormalizerSymbol(Symbol):
    """Normalizer type representation class of groonga"""


class Normalizer(object):
    """Normalizer"""

    NormalizerAuto = NormalizerSymbol('NormalizerAuto')
    NormalizerNFKC51 = NormalizerSymbol('NormalizerNFKC51')


class LogLevelSymbol(Symbol):
    """Log lovel representation class of groonga"""


class LogLevel(object):
    """Log level"""

    EMERG = LogLevelSymbol('EMERG')
    ALERT = LogLevelSymbol('ALERT')
    CRIT = LogLevelSymbol('CRIT')
    error = LogLevelSymbol('error')
    warning = LogLevelSymbol('warning')
    notice = LogLevelSymbol('notice')
    info = LogLevelSymbol('info')
    debug = LogLevelSymbol('debug')
    EMERGENCY = EMERG  # alias
    CRITICAL = CRIT  # alias
    WARNING = warning  # alias
    NOTICE = notice  # alias
    INFO = info  # alias
    DEBUG = debug  # alias


class SuggestTypeFlag(Flags):
    """Type flags representation class of groonga's suggest"""


class SuggestType(object):
    """Suggest types"""

    complete = SuggestTypeFlag(Symbol('complete'))
    correct = SuggestTypeFlag(Symbol('correct'))
    suggest = SuggestTypeFlag(Symbol('suggest'))
    COMPLETE = complete  # alias
    CORRECT = correct  # alias
    SUGGEST = suggest  # alias
