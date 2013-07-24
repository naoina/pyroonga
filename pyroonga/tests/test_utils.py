# -*- coding: utf-8 -*-

import unittest

from pyroonga import utils


class TestEscape(unittest.TestCase):
    def test_escape(self):
        result = utils.escape('https://github.com/naoina/pyroonga')
        self.assertEqual(result, 'https://github.com/naoina/pyroonga')

        result = utils.escape('left "center" right\nhello \\yen')
        self.assertEqual(result, r'left \"center\" right\nhello \\yen')


class TestToPython(unittest.TestCase):
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
        self.assertEqual(result, expected)

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
        self.assertEqual(result, expected)

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
        self.assertEqual(result, expected)


def main():
    unittest.main()

if __name__ == '__main__':
    main()
