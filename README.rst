pyroonga |travis-ci-status|_
============================

What's this?
------------

Python interface for `groonga <http://groonga.org/>`_ fulltext search engine.

Requirements
------------

- Python 2.6 or 3.x and later
- groonga

Installation
------------

from pypi::

   % pip install pyroonga

from source::

   % python setup.py install

Usage
-----

First, Please run ``groonga`` by server mode or daemon mode. see following::

   # server mode
   % groonga -s DB_PATH_NAME

   # daemon mode
   % groonga -d DB_PATH_NAME

See ``groonga --help`` for more options.

Create Table
^^^^^^^^^^^^

::

   from pyroonga import tablebase, Column, Groonga

   # create the base class for table definition.
   Table = tablebase()

   # define the table
   class Site(Table):
      title = Column()
      name = Column()

   class Blog(Table):
      entry = Column()

   # create and bind the groonga connection object
   grn = Groonga()
   Table.bind(grn)

   # create the all table on groonga's database
   Table.create_all()

Data loading
^^^^^^^^^^^^

::

   data = [Site(_key='key1', title='foo', name='hoge'),
           Site(_key='key2', title='bar', name='fuga'),
           Site(_key='key3', title='baz', name='piyo'),
           Site(_key='key4', title='qux', name='xyzzy')]

   Site.load(data)

The example above is load the data to groonga immediately.
Also pseudo commit and rollback are supported::

   data1 = [Site(_key='key5', title='Constellation', name='Sagittarius'),
            Site(_key='key6', title='Constellation', name='Pisces')]

   # first load, but not load to groonga actually
   data = Site.load(data1, immediate=False)

   data2 = [Site(_key='key7', title='Constellation', name='Aquarius')]
   data.load(data2)  # same as previous

   # load data to groonga actually
   data.commit()

Or reset loaded data::

   data.rollback()

Note that loaded data reset is only if set ``immediate=False``\ .

Query and get data as a mapped object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get the all data from ``Site`` table::

   data = Site.select().all()

And print the data::

   for row in data:
       print(row._id, row._key, row.title)

Fulltext search queries::

   Site.select().match_columns(Site.title).query('foo').all()
   Site.select().match_columns(Site.title, Site.name).query('bar').all()

The above example is same as following queries::

   select --table Site --match_columns 'title' --query "foo"
   select --table Site --match_columns 'title OR name' --query "bar"

For more complex queries using `pyroonga.odm.GE`::

   from pyroonga.odm import GE

   Site.select().match_columns(Site.title).query(GE('foo') | GE('bar')).all()

The above example is same as following query::

   select --table Site --match_columns 'title' --query "(foo OR bar)"

And also not use `match_columns`::

   Site.select(title='foo').all()
   Site.select(title='foo', name='bar').all()  # "or" search

The above example is same as following queries::

   select --table Site --query "(title:@\"foo\")"
   select --table Site --query "(title:@\"foo\" OR name:@\"bar\")"

Conditional search query::

   Site.select(Site.title == 'bar').all()

Conbination for a condition::

   Site.select((Site._id > 3) & (Site.title == 'baz')).all()

Limit and offset::

   Site.select().limit(3).offset(2).all()

Sortby::

   Site.select().sortby(Site._id).all()   # asc
   Site.select().sortby(-Site._id).all()  # desc

Select the output columns::

   # get the title and name columns
   Site.select().output_columns(Site.title, Site.name).all()

   # get the all columns
   Site.select().output_columns(Site.ALL).all()

Drilldown
"""""""""

Switch to the drilldown query after the call of drilldown() from select() method chain::

   data = Site.select().sortby(Site._key).drilldown(Site.title).all()

Result of drilldown will be stored to the ``drilldown`` attribute of the return value from all() method::

   for drilldown in data.drilldown:
       print(drilldown._key, drilldown._nsubrecs)

A ``sortby()`` method in example above, It is query option of ``--sortby``\ .
For sortby of drilldown, Please call of ``sortby()`` method after the call of ``drilldown()`` method::

   Site.select().drilldown(Site.title).sortby(Site._key).all()

A ``sortby()`` method in example above, It is query option of ``--drilldown_sortby``\ .
Of course, As well as ``limit()`` , ``offset()`` and ``output_columns()`` methods.

Suggest
^^^^^^^

**N.B.** The Groonga's suggest feature is still in draft.

First, Create table if still not created::

   from pyroonga import SuggestTable

   grn = Groonga()
   SuggestTable.bind(grn)
   SuggestTable.create_all()

Second, Data loading::

   import time
   from pyroonga import event_query

   data = [event_query(time=time.time(), sequence=1, item='e'),
           event_query(time=time.time(), sequence=1, item='en'),
           event_query(time=time.time(), sequence=1, item='eng'),
           event_query(time=time.time(), sequence=1, item='engi'),
           event_query(time=time.time(), sequence=1, item='engin'),
           event_query(time=time.time(), sequence=1, item='engine', type='submit')]
   event_query.load(data)

Finally, Querying::

   from pyroonga import item_query, SuggestType

   query = 'en'
   result = item_query.suggest(query).types(SuggestType.complete). \
           frequency_threshold(1).all()
   for r in result.complete:
       print("key is '%s', score is %s" % (r._key, r._score))

See also http://groonga.org/docs/suggest.html

More information
^^^^^^^^^^^^^^^^

Still not written.

See also
--------

http://groonga.org/ (Japanese: http://groonga.org/ja/ )

LICENSE
-------

pyroonga is licensed under the MIT license.

.. |travis-ci-status| image:: https://travis-ci.org/naoina/pyroonga.png?branch=master
.. _`travis-ci-status`: https://travis-ci.org/naoina/pyroonga
