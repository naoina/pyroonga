# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    text_type = unicode
else:
    text_type = str


def python_2_unicode_compatible(klass):
    """This decorator from on Django

    See https://docs.djangoproject.com/en/dev/ref/utils/#django.utils.encoding.python_2_unicode_compatible
    """
    if PY2:
        klass.__unicode__ = klass.__str__
        klass.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return klass


def to_text(s):
    """Convert to text object

    :param s: object
    :returns: text object (``str`` on Python3 and ``unicode`` on Python2)
    """
    if isinstance(s, text_type):
        return s
    if isinstance(s, bytes):
        return s.decode('utf-8')
    if hasattr(s, '__unicode__'):
        return s.__unicode__()
    return text_type(s)


def escape(s, force_quote=False):
    """Escape for query of groonga

    :param s: string
    :param force_quote: If True, always quote the ``s``.
        If False, quote only if contains '\u0020'.  Default is False
    :returns: escaped string
    """
    s = s.replace('\\', r'\\')
    s = s.replace('\n', r'\n')
    s = s.replace('"', r'\"')
    if force_quote or ' ' in s:
        s = '"%s"' % s
    return s


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
