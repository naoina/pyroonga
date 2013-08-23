# -*- coding: utf-8 -*-

import pytest

from pyroonga import utils


class ToTextUnicodeHelper(object):
    def __unicode__(self):
        return u'さくら咲き'


@pytest.mark.parametrize(('value', 'expected'), (
    ('value', u'value'),
    (b'value', u'value'),
    (u'value', u'value'),
    ('さくら咲き', u'さくら咲き'),
    (u'さくら咲き'.encode('utf-8'), u'さくら咲き'),
    (u'さくら咲き', u'さくら咲き'),
    (10, u'10'),
    (ToTextUnicodeHelper(), u'さくら咲き'),
))
def test_to_text(value, expected):
    assert utils.to_text(value) == expected


@pytest.mark.parametrize(('values', 'expected'), (
    (['https://github.com/naoina/pyroonga', False],
     'https://github.com/naoina/pyroonga'),
    (['https://github.com/naoina/pyroonga', True],
     '"https://github.com/naoina/pyroonga"'),
    (['left "center" right\nhello \\yen', False],
     r'"left \"center\" right\nhello \\yen"'),
    (['left "center" right\nhello \\yen', True],
     r'"left \"center\" right\nhello \\yen"'),
    (['さくら咲き', False], 'さくら咲き'),
    (['さ\\く ら"咲\nき', False], r'"さ\\く ら\"咲\nき"'),
    ([u'さくら咲き', False], u'さくら咲き'),
    ([u'さ\\く ら"咲\nき', False], u'"さ\\\\く ら\\"咲\\nき"'),
    (['さくら咲き', True], '"さくら咲き"'),
    (['さ\\く ら"咲\nき', True], r'"さ\\く ら\"咲\nき"'),
    ([u'さくら咲き', True], u'"さくら咲き"'),
    ([u'さ\\く ら"咲\nき', True], u'"さ\\\\く ら\\"咲\\nき"'),
))
def test_escape(values, expected):
    result = utils.escape(*values)
    assert result == expected


class TestToPython(object):
    def test_with_base_idx_is_zero(self):
        values = [[
            ["id", "UInt32"],
            ["name", "ShortText"],
            ["path", "ShortText"],
            ["flags", "ShortText"],
            ["domain", "ShortText"],
            ["range", "ShortText"],
            ["default_tokenizer", "ShortText"],
            ["normalizer", "ShortText"]],
            [256,
             "TestTable",
             "a.db.0000100",
             "TABLE_HASH_KEY|PERSISTENT",
             "ShortText",
             None,
             None,
             None]]
        expected = [{
            'id': 256,
            'name': 'TestTable',
            'path': 'a.db.0000100',
            'flags': 'TABLE_HASH_KEY|PERSISTENT',
            'domain': 'ShortText',
            'range': None,
            'default_tokenizer': None,
            'normalizer': None,
            }]
        result = utils.to_python(values, 0)
        assert result == expected

    def test_with_base_idx_is_one(self):
        values = [[6],
                  [['_id', 'UInt32'],
                   ['item', 'item_query'],
                   ['sequence', 'sequence_query'],
                   ['time', 'Time'],
                   ['type', 'event_type']],
                  [1, 's', 'deadbeef', 12.0, ''],
                  [2, 'se', 'deadbeef', 13.0, ''],
                  [3, 'sea', 'deadbeef', 14.0, 'submit']]
        expected = [
            {'_id': 1,
             'item': 's',
             'sequence': 'deadbeef',
             'time': 12.0,
             'type': ''},
            {'_id': 2,
             'item': 'se',
             'sequence': 'deadbeef',
             'time': 13.0,
             'type': ''},
            {'_id': 3,
             'item': 'sea',
             'sequence': 'deadbeef',
             'time': 14.0,
             'type': 'submit'}]
        result = utils.to_python(values, 1)
        assert result == expected

    def test_with_maxlen(self):
        values = [[['_id', 'UInt32'],
                   ['item', 'item_query'],
                   ['sequence', 'sequence_query'],
                   ['time', 'Time'],
                   ['type', 'event_type']],
                  [1, 's', 'deadbeef', 12.0, ''],
                  [2, 'se', 'deadbeef', 13.0, ''],
                  [3, 'sea', 'deadbeef', 14.0, 'submit']]
        expected = [
            {'_id': 1,
             'item': 's',
             'sequence': 'deadbeef',
             'time': 12.0,
             'type': ''},
            {'_id': 2,
             'item': 'se',
             'sequence': 'deadbeef',
             'time': 13.0,
             'type': ''}]
        result = utils.to_python(values, 0, maxlen=2)
        assert result == expected
