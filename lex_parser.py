#!/usr/bin/env python
# coding: utf8
#
#
# @file:    lex_parser
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2015/01/19 18:18:24

from ply import lex

reserved = {
    'select': 'SELECT',
    'from': 'FROM',
    'where': 'WHERE',
    'and': 'AND',
    'order': 'ORDER',
    'by': 'BY',
    'asc': 'ASC',
    'desc': 'DESC',
    'limit': 'LIMIT',
    'or': 'OR',
    'not': 'NOT',
    'like': 'LIKE',
    # accumulative functions
    'max': 'MAX',
    'min': 'MIN',
    'avg': 'AVG',
    'sum': 'SUM',
    'count': 'COUNT',
    # fields name
    'name': 'NAME',
    'size': 'SIZE',
    'ctime': 'CTIME',
    'mtime': 'MTIME',
    'atime': 'ATIME'
}

tokens = [
    'QUOTE',
    'LE',       # '<='
    'GE',       # '>='
    'NE',       # '!='
    'TIME',
    'DATE',
    'NUMBER',
    'FNAME',
    ] + reserved.values()

literals = '=()*<>\'",'

t_SELECT = r'(select)|(SELECT)'
t_FROM = r'(from)|(FROM)'
t_WHERE = r'(where)|(WHERE)'
t_AND = r'(and)|(AND)'
t_ORDER = r'(order)|(ORDER)'
t_BY = r'(by)|(BY)'
t_ASC = r'(asc)|(ASC)'
t_DESC = r'(desc)|(DESC)'
t_LIMIT = r'(limit)|(LIMIT)'
t_OR = r'(or)|(OR)'
t_NOT = r'(not)|(NOT)'
t_LE = r'<='
t_GE = r'>='
t_NE = r'!='
t_QUOTE = r'(\')|"'
t_LIKE = r'(like)|(LIKE)'
t_MAX = r'(max)|(MAX)'
t_MIN = r'(min)|(MIN)'
t_AVG = r'(avg)|(AVG)'
t_SUM = r'(sum)|(SUM)'
t_COUNT = r'(count)|(COUNT)'
t_NAME = r'(\name)|(\NAME)'
t_SIZE = r'(\size)|(\SIZE)'
t_CTIME = r'(\ctime)|(\CTIME)'
t_MTIME = r'(\mtime)|(\MTIME)'
t_ATIME = r'(\atime)|(\ATIME)'
t_ignore = ' \t\n'


def t_TIME(t):
    r'\d{2}:\d{2}:\d{2}'
    return t


def t_DATE(t):
    r'\d{4}-\d{2}-\d{2}'
    return t


def t_NUMBER(t):
    r'0|(\d+)\.?(\d+)?'
    t.value = int(t.value)
    return t


def t_FNAME(t):
    r'[^ \t\n=\(\)\*\<\>\'",]+'
    if t.value in reserved:
        t.type = reserved[t.value]
        return t
    lower_case = t.value.lower()

    if lower_case in reserved:
        t.type = reserved[lower_case]
        return t

    return t


def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)


def t_error(t):
    print 'failed to lex parse, line: %d, input: [%s]' % (t.lexer.lineno,
                                                          t.value)

lexer = lex.lex()

if __name__ == '__main__':
    stmt = 'select * from . where ctime < 2015-01-20 16:55:00 and name = \'test\' order by name asc'
    lexer.input(stmt)
    for t in lexer:
        print t
