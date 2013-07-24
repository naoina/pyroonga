# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]


def escape(s):
    """Escape for query of groonga

    :param s: string
    :returns: escaped string
    """
    return s.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"')


def to_python(results, base_idx, maxlen=None):
    """Convert from results of query to Python objects

    :param results: query results
    :param base_idx: index of start of table info
    :param maxlen: maximum length of mapping results. Default is all
    :returns: list of mapped dict of query results
    """
    cols = [col[0] for col in results[base_idx]]
    colrange = range(len(cols))
    objs = []
    if maxlen is not None:
        maxlen += 1
    # TODO: implements by generator
    for v in results[base_idx + 1:maxlen]:
        mapped = dict(zip(cols, [v[i] for i in colrange]))
        objs.append(mapped)
    return objs
