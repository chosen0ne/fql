#!/usr/bin/env python
# coding: utf8
#
#
# @file:    grammar_parser
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2015/01/19 22:08:25

import re
import time
import accu_func
from datetime import datetime
from collections import OrderedDict
from ply import yacc
from lex_parser import *

'''
grammar:
    statement : SELECT select_statement from_statement where_statement
              | SELECT select_statement where_statement
              | SELECT select_statement from_statement
              | SELECT from_statement where_statement
              | SELECT where_statement
              | SELECT select_statement
              | SELECT from_statement
              | statement order_statement

    select_statement : fields_select_stmt
                     | accu_func_stmt

    from_statement : FROM FNAME

    where_statement : WHERE condition_statement

    order_statement : ORDER BY order_factor
                    | order_statement, order_factor

    a_field : NAME
            | SIZE
            | CTIME
            | MTIME
            | ATIME

    accu_field : ATIME
               | MTIME
               | CTIME
               | SIZE

    fields_select_stmt : fields_select_stmt ','  a_field
                       | a_field
                       | '*'

    accu_func_stmt : AVG '(' accu_field ')'
                   | MAX '(' accu_field ')'
                   | MIN '(' accu_field ')'
                   | COUNT '(' '*' ')'
                   | SUM '(' '*' ')'
                   | accu_func_stmt ',' AVG '(' accu_field ')'
                   | accu_func_stmt ',' MAX '(' accu_field ')'
                   | accu_func_stmt ',' MIN '(' accu_field ')'
                   | accu_func_stmt ',' COUNT '(' '*' ')'
                   | accu_func_stmt ',' SUM '(' SIZE ')'

    condition_statement : condition_statement OR and_condition
                        | and_condition

    and_condition : and_condition AND factor
                  | factor

    factor : name_factor
           | size_factor
           | ctime_factor
           | mtime_factor
           | atime_factor
           | '(' condition_statement ')'
           | NOT factor

    name_factor : NAME '=' QUOTE FNAME QUOTE
                | NAME LIKE QUOTE FNAME QUOTE

    size_factor : SIZE '=' NUMBER
                | SIZE '>' NUMBER
                | SIZE '<' NUMBER
                | SIZE NE NUMBER
                | SIZE GE NUMBER
                | SIZE LE NUMBER

    datetime_factor : DATE
                    | DATE TIME

    ctime_factor : CTIME '=' datetime_factor
                 | CTIME '>' datetime_factor
                 | CTIME GE datetime_factor
                 | CTIME '<' datetime_factor
                 | CTIME LE datetime_factor
                 | CTIME NE datetime_factor

    mtime_factor : MTIME '=' datetime_factor
                 | MTIME '>' datetime_factor
                 | MTIME GE datetime_factor
                 | MTIME '<' datetime_factor
                 | MTIME LE datetime_factor
                 | MTIME NE datetime_factor

    atime_factor : ATIME '=' datetime_factor
                 | ATIME '>' datetime_factor
                 | ATIME GE datetime_factor
                 | ATIME '<' datetime_factor
                 | ATIME LE datetime_factor
                 | ATIME NE datetime_factor

    order_factor : a_field
                 | a_field ASC
                 | a_field DESC

'''

# used to compare file stats, such as st_size, st_ctime, st_atime...
cmp_operators = {
        '=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) == val,
        '>': lambda field, val: lambda finfo: getattr(finfo['stat'], field) > val,
        '<': lambda field, val: lambda finfo: getattr(finfo['stat'], field) < val,
        '!=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) != val,
        '>=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) >= val,
        '<=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) <= val
}


def p_statement(p):
    '''
        statement : SELECT select_statement from_statement where_statement
                  | SELECT select_statement where_statement
                  | SELECT select_statement from_statement
                  | SELECT from_statement where_statement
                  | SELECT where_statement
                  | SELECT select_statement
                  | SELECT from_statement
                  | statement order_statement
    '''
    if not p[0]:
        p[0] = {}

    stmts = p[0]
    if len(p) == 5:
        stmts['select'], stmts['from'], stmts['where'] = p[2][1], p[3][1], p[4][1]

    elif len(p) == 4:
        stmt_type, stmt = p[2]
        if stmt_type == 'select':
            stmts['select'] = p[2][1]
            stmts[p[3][0]] = p[3][1]
        else:
            stmts['from'], stmts['where'] = p[2][1], p[3][1]

    elif len(p) == 3:
        stmt_type, stmt = p[2]
        if stmt_type == 'where' or stmt_type == 'select' or stmt_type == 'from':
            stmts[stmt_type] = stmt
        else:
            # statement : statement order_statement
            stmt = p[1]
            for k, v in stmt.items():
                stmts[k] = v

            # check conflict between order by and aggregation function
            if stmts['select'][0] == 'aggregate':
                raise Exception('Confliction between order by and select aggregation function')

            stmts['order'] = p[2][1]


def p_select_stmt(p):
    '''
        select_statement : fields_select_stmt
                         | accu_func_stmt
    '''
    # format of p[0]: ('select', ('field', [])) or ('select', ('accu', []))
    p[0] = ('select', p[1])


def p_fields_select_stmt(p):
    '''
        fields_select_stmt : fields_select_stmt ','  a_field
                           | a_field
                           | '*'
    '''
    if not p[0]:
        p[0] = ('field', [])

    fields = p[0][1]
    if len(p) == 4:
        fields.extend(p[1])
        fields.append(p[3])
    elif len(p) == 2:
        fields.append(p[1])


def p_a_field(p):
    '''
        a_field : NAME
                | SIZE
                | CTIME
                | MTIME
                | ATIME
    '''
    p[0] = p[1].lower()


def p_accu_field(p):
    '''
        accu_field : ATIME
                   | MTIME
                   | CTIME
                   | SIZE
    '''
    p[0] = p[1]


def p_accu_func_stmt1(p):
    '''
        accu_func_stmt : AVG '(' accu_field ')'
                       | MAX '(' accu_field ')'
                       | MIN '(' accu_field ')'
                       | COUNT '(' '*' ')'
                       | SUM '(' SIZE ')'
    '''
    f = 'st_' + p[3].lower()
    op = p[1]
    accu_obj_name = op[0].upper() + op[1:].lower() + 'FuncCls'
    accu_obj = accu_func.__dict__[accu_obj_name]
    p[0] = ('aggregate', [accu_obj(f)])


def p_accu_func_stmt2(p):
    '''
        accu_func_stmt : accu_func_stmt ',' AVG '(' accu_field ')'
                       | accu_func_stmt ',' MAX '(' accu_field ')'
                       | accu_func_stmt ',' MIN '(' accu_field ')'
                       | accu_func_stmt ',' COUNT '(' '*' ')'
                       | accu_func_stmt ',' SUM '(' SIZE ')'
    '''
    if not p[0]:
        p[0] = []

    f = 'st_' + p[5].lower()
    op = p[3]
    accu_obj_name = op[0].upper() + op[1:].lower() + 'FuncCls'
    accu_obj = accu_func.__dict__[accu_obj_name]

    p[0].extend(p[1])
    p[0].append(accu_obj(f))


def p_from_stmt(p):
    'from_statement : FROM FNAME'
    p[0] = ('from', p[2])


def p_where_stmt(p):
    'where_statement : WHERE condition_statement'
    p[0] = ('where', p[2])


def p_condition_stmt1(p):
    'condition_statement : condition_statement OR and_condition'
    p1, p2 = p[1], p[3]
    p[0] = lambda finfo: p1(finfo) or p2(finfo)


def p_condition_stmt2(p):
    'condition_statement : and_condition'
    p[0] = p[1]


def p_and_condition1(p):
    'and_condition : and_condition AND factor'
    p1, p2 = p[1], p[3]
    p[0] = lambda finfo: p1(finfo) and p2(finfo)


def p_and_condition2(p):
    'and_condition : factor'
    p[0] = p[1]


def p_factor(p):
    '''
        factor : name_factor
               | size_factor
               | ctime_factor
               | mtime_factor
               | atime_factor
               | '(' condition_statement ')'
               | NOT factor
    '''
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 3:
        p1 = p[2]
        p[0] = lambda finfo: not p1(finfo)
    elif len(p) == 4:
        p[0] = p[2]


def p_name_factor(p):
    '''
        name_factor : NAME '=' QUOTE FNAME QUOTE
                    | NAME LIKE QUOTE FNAME QUOTE
    '''
    _, _, op, _, fname, _ = p
    if op == '=':
        p[0] = lambda finfo: finfo['name'] == fname
    else:
        fname = fname.replace('.', '\.')
        fname = fname.replace('%', '.*')
        pattern = re.compile(fname)
        p[0] = lambda finfo: pattern.match(finfo['name']) is not None


def p_size_factor(p):
    '''
        size_factor : SIZE '=' NUMBER
                    | SIZE '>' NUMBER
                    | SIZE '<' NUMBER
                    | SIZE NE NUMBER
                    | SIZE GE NUMBER
                    | SIZE LE NUMBER
    '''
    _, _, op, fsize = p
    cmp_func = cmp_operators[op]
    p[0] = cmp_func('st_size', fsize)


def p_datetime_factor(p):
    '''
        datetime_factor : DATE
                        | DATE TIME
    '''
    if len(p) == 2:
        p[0] = datetime.strptime(p[1], '%Y-%m-%d')
    else:
        p[0] = datetime.strptime(p[1] + ' ' + p[2], '%Y-%m-%d %H:%M:%S')

def p_ctime_factor(p):
    '''
        ctime_factor : CTIME '=' datetime_factor
                     | CTIME '>' datetime_factor
                     | CTIME GE datetime_factor
                     | CTIME '<' datetime_factor
                     | CTIME LE datetime_factor
                     | CTIME NE datetime_factor
    '''
    time_proc(p)


def p_mtime_factor(p):
    '''
        mtime_factor : MTIME '=' datetime_factor
                     | MTIME '>' datetime_factor
                     | MTIME GE datetime_factor
                     | MTIME '<' datetime_factor
                     | MTIME LE datetime_factor
                     | MTIME NE datetime_factor
    '''
    time_proc(p)


def p_atime_factor(p):
    '''
        atime_factor : ATIME '=' datetime_factor
                     | ATIME '>' datetime_factor
                     | ATIME GE datetime_factor
                     | ATIME '<' datetime_factor
                     | ATIME LE datetime_factor
                     | ATIME NE datetime_factor
    '''
    time_proc(p)


def p_order_statement(p):
    '''
        order_statement : ORDER BY order_factor
                        | order_statement ',' order_factor
    '''
    # list of tuple(field, asc/desc)
    if not p[0]:
        p[0] = ('order', OrderedDict())

    orders = p[0][1]

    field, ad = p[3]
    if field in orders:
        raise Exception('Duplicated field of ORDER BY, field: %s' % field)
    orders[field] = ad

    if type(p[1]) is tuple:
        # order_statement ',' order_factor
        for k, v in p[1][1].items():
            if k in orders:
                raise Exception('Duplicated field of ORDER BY, field: %s' % k)
            orders[k] = v


def p_order_factor(p):
    '''
        order_factor : a_field
                     | a_field ASC
                     | a_field DESC
    '''
    # (field_name, asc/desc)
    p[0] = (p[1], p[2].lower() if len(p) == 3 else 'asc')


def time_proc(p):
    _, field_name, op, d = p
    d = time.mktime(d.timetuple())
    field_name = 'st_' + field_name.lower()

    cmp_func = cmp_operators[op]
    p[0] = cmp_func(field_name, d)


def p_error(p):
    if not p:
        print 'End of file'
        return
    print 'parse error, unexpected token:', p.type


parser = yacc.yacc()


if __name__ == '__main__':
    yacc.yacc()
    stmt = 'select name from . where size > 1 order by name'
    print parser.parse(stmt)
