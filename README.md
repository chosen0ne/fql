# fql
=========

'FQL' is the shortened name for File Query Language. It looks like SQL, but used to query file information. Current version is '0.1.0'.

####File Attributes:
    name: file name
    ctime: created time
    mtime: last modified time
    atime: last accessed time
    size: file size

    Lower case and upper case of the attributes name are both supported.

####Aggregate Function
    FQL supports aggregate function, as sql does. At present, aggregate functions FQL suppoted are:
        - count
        - sum
        - max
        - min

####Usage:
    fql.py is the entry point of the application. It supported two ways:
        1) Line-oriented command interpreter.
            python fql.py
        2) One command at a time
            python fql.py 'select * from .'

#### Example:
    FQL is SQL.
    1) list all the files of current directory and the sub-directories.
        > python fql.py 'select * from .'
        > -----------------------------------------------------------------------------------------------------
          | name                | ctime               | mtime               | atime               | size      |
          -----------------------------------------------------------------------------------------------------
          | accu_func.py        | 2015-01-22 16:05:07 | 2015-01-22 16:05:07 | 2015-01-23 19:53:00 | 2.21K     |
          -----------------------------------------------------------------------------------------------------
          | accu_func.pyc       | 2015-01-23 19:53:00 | 2015-01-23 19:53:00 | 2015-01-23 19:53:00 | 5.87K     |
          -----------------------------------------------------------------------------------------------------
          | fql.py              | 2015-01-23 19:24:58 | 2015-01-23 19:24:58 | 2015-01-23 19:53:00 | 1.44K     |
          -----------------------------------------------------------------------------------------------------

    2) list all the python files name and size.
        > python fql.py 'select name, size from . where name like "%.py$"'
        > -----------------------------------
          | name                | size      |
          -----------------------------------
          | accu_func.py        | 2.21K     |
          -----------------------------------
          | fql.py              | 1.44K     |
          -----------------------------------

    3) list all the python files which are created after '2015-01-23 19:50:00'
        > python fql.py 'select * from . where name like "%.py$" and ctime > 2015-01-23 19:50:00'
        > -----------------------------------------------------------------------------------------------------
          | name                | ctime               | mtime               | atime               | size      |
          -----------------------------------------------------------------------------------------------------
          | fql.py              | 2015-01-23 19:54:50 | 2015-01-23 19:54:49 | 2015-01-23 19:57:25 | 1.41K     |
          -----------------------------------------------------------------------------------------------------
          | parsetab.py         | 2015-01-23 19:53:00 | 2015-01-23 19:53:00 | 2015-01-23 19:54:00 | 17.87K    |
          -----------------------------------------------------------------------------------------------------

    4) list all the python files which are created after '2015-01-23 19:50:00', and the files named 'README.md'
        > python fql.py 'select * from . where name like "%.py$" and ctime > 2015-01-23 19:50:00'
        > -----------------------------------------------------------------------------------------------------
          | name                | ctime               | mtime               | atime               | size      |
          -----------------------------------------------------------------------------------------------------
          | fql.py              | 2015-01-23 19:54:50 | 2015-01-23 19:54:49 | 2015-01-23 20:01:31 | 1.41K     |
          -----------------------------------------------------------------------------------------------------
          | parsetab.py         | 2015-01-23 19:53:00 | 2015-01-23 19:53:00 | 2015-01-23 19:54:00 | 17.87K    |
          -----------------------------------------------------------------------------------------------------
          | README.md           | 2015-01-23 20:00:53 | 2015-01-23 20:00:53 | 2015-01-23 20:00:53 | 3.94K     |
          -----------------------------------------------------------------------------------------------------

    5) calculate the size of all the python files.
        > python fql.py 'select sum(size) from . where name like "%.py$"'
        > -----------------------------------------
          | sum(st_size)    | 38.54K              |
          -----------------------------------------

    6) list the file which is created latest.
        > python fql.py 'select max(ctime) from .'
        > ------------------------------------------
          | max(st_ctime)    | 2015-01-23 20:07:49 |
          ------------------------------------------

