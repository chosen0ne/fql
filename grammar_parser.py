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
              | statement limit_statement
              | statement group_by_statement

    select_statement : select_factor
                     | select_statement ',' select_factor

    select_factor : a_field
                  | '*'
                  | accu_func_factor
                  | group_func_factor
                  | select_factor as FNAME

    from_statement : FROM FNAME

    where_statement : WHERE condition_statement

    order_statement : ORDER BY order_factor
                    | order_statement, order_factor

    limit_statement : LIMIT NUMBER
                    | LIMIT NUMBER ',' NUMBER

    a_field : NAME
            | PATH
            | SIZE
            | CTIME
            | MTIME
            | ATIME

    accu_field : ATIME
               | MTIME
               | CTIME
               | SIZE

    accu_func : AVG
              | MAX
              | MIN
              | SUM

    accu_func_factor : accu_func '(' accu_field ')'
                     | COUNT '(' accu_field ')'
                     | COUNT '(' '*' ')'

    condition_statement : condition_statement OR and_condition
                        | and_condition

    and_condition : and_condition AND factor
                  | factor

    factor : name_factor
           | size_factor
           | time_factor
           | alias_factor
           | '(' condition_statement ')'
           | NOT factor

    name_factor : NAME '=' QUOTE FNAME QUOTE
                | NAME NE QUOTE FNAME QUOTE
                | NAME LIKE QUOTE FNAME QUOTE

    cmp_op_sub_factor : '='
                      | '>'
                      | '<'
                      | NE
                      | GE
                      | LE

    size_factor : SIZE cmp_op_sub_factor NUMBER

    datetime_factor : DATE
                    | DATE TIME

    time_field : CTIME
               | MTIME
               | ATIME

    time_factor : time_field cmp_op_sub_factor datetime_factor

    alias_factor : FNAME cmp_op_sub_factor NUMBER
                 | FNAME cmp_op_sub_factor datetime_factor

    order_sub_factor : a_field
                     | accu_func_factor
                     | group_func_factor
                     | FNAME

    order_factor : order_sub_factor
                 | order_sub_factor ASC
                 | order_sub_factor DESC

    group_func_factor : MINUTE '(' time_field ')'
                      | HOUR '(' time_field ')'
                      | DAY '(' time_field ')'
                      | MONTH '(' time_field ')'
                      | YEAR '(' time_field ')'
                      | FTYPE

    having_statement : HAVING having_condition

    having_condition : having_condition OR having_and_factor
                     | having_and_factor

    having_and_factor : having_and_factor AND having_factor
                      | having_factor

    having_sub_factor : accu_func_factor
                      | FNAME

    having_factor : having_sub_factor cmp_op_sub_factor NUMBER
                  | '(' having_condition ')'
                  | NOT having_factor

    group_by_statement : GROUP BY group_func_factor
                       | GROUP BY FNAME
                       | GROUP BY group_func_factor having_statement
                       | GROUP BY FNAME having_statement

'''


def fstat_cmp_op(f, val, op):
    def fstat_cmp(finfo, alias=None):
        field = alias['from_alias'][f] if alias and f in alias['from_alias'] \
            else f

        stat = int(getattr(finfo['stat'], 'st_' + field))
        if op == '=':
            return stat == val
        elif op == '>':
            return stat > val
        elif op == '<':
            return stat < val
        elif op == '!=':
            return stat != val
        elif op == '>=':
            return stat >= val
        elif op == '<=':
            return stat <= val
        else:
            raise Exception('Unsupport operator')

    return fstat_cmp


# used to compare file stats, such as st_size, st_ctime, st_atime...
fstat_cmp_operators = {
    '=': lambda field, val: fstat_cmp_op(field, val, '='),
    '>': lambda field, val: fstat_cmp_op(field, val, '>'),
    '<': lambda field, val: fstat_cmp_op(field, val, '<'),
    '!=': lambda field, val: fstat_cmp_op(field, val, '!='),
    '>=': lambda field, val: fstat_cmp_op(field, val, '>='),
    '<=': lambda field, val: fstat_cmp_op(field, val, '<='),
}


time_aggregate_operators = {
    'minute': lambda field: lambda finfo: datetime.fromtimestamp(
        getattr(finfo['stat'], field)).strftime('%Y-%m-%d %H:%M'),
    'hour': lambda field: lambda finfo: datetime.fromtimestamp(
        getattr(finfo['stat'], field)).strftime('%Y-%m-%d %H'),
    'day': lambda field: lambda finfo: datetime.fromtimestamp(
        getattr(finfo['stat'], field)).strftime('%Y-%m-%d'),
    'month': lambda field: lambda finfo: datetime.fromtimestamp(
        getattr(finfo['stat'], field)).strftime('%Y-%m'),
    'year': lambda field: lambda finfo: datetime.fromtimestamp(
        getattr(finfo['stat'], field)).strftime('%Y'),
}


# fetch file type '.*$'
def ftype_aggregate_operator(finfo):
    idx = finfo['name'].rfind('.')
    return finfo['name'][idx:] if idx != -1 else '$'


def check_order_stmt(stmts, order_stmt):
    if 'order' in stmts:
        raise Exception('Duplicated order by, exists: order by %s, here: order'
                        ' by %s' % (stmts['order'].items(),
                                    order_stmt.items()))

    if 'limit' in stmts:
        raise Exception('\'limit\' must be used behind \'order by\'')


def check_limit_stmt(stmts, limit_stmt):
    if 'limit' in stmts:
        raise Exception('Duplicated limit, exists: limit %s, here: limit %s' %
                        (stmts['limit'], limit_stmt))


def check_group_stmt(stmts, group_stmt):
    if 'group' in stmts:
        raise Exception('Duplicated group by')

    if 'order' in stmts:
        raise Exception('\'order by\' must be used behind \'group by\'')

    if 'limit' in stmts:
        raise Exception('\'limit\' must be used behind \'group by\'')


def check_select_stmt(stmts):
    if 'select' not in stmts:
        return

    select_stmt = stmts['select']
    if 'field' in select_stmt and 'aggregations' in select_stmt:
        raise Exception('fields and aggregations can\'t be selected at the'
                        ' same time')

    if 'field' in select_stmt and 'dimension_aggr' in select_stmt:
        raise Exception('fields and dimension can\'t be selected at the'
                        ' same time')


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
                  | statement limit_statement
                  | statement group_by_statement
    '''
    if isinstance(p[1], dict):
        # statement : ^statement.*
        p[0] = p[1]
    else:
        p[0] = {}

    stmts = p[0]
    if len(p) == 5:
        stmts['select'], stmts['from'], stmts['where'] = \
            p[2][1], p[3][1], p[4][1]

    elif len(p) == 4:
        stmt_type, stmt = p[2]
        if stmt_type == 'select':
            stmts['select'] = p[2][1]
            stmts[p[3][0]] = p[3][1]
        elif stmt_type == 'from':
            stmts['from'], stmts['where'] = p[2][1], p[3][1]

    elif len(p) == 3:
        stmt_type, stmt = p[2]
        if stmt_type == 'where' or stmt_type == 'select' or \
                stmt_type == 'from':
            stmts[stmt_type] = stmt
        elif stmt_type == 'order':
            # statement : statement order_statement
            check_order_stmt(stmts, p[2][1])

            stmts['order'] = p[2][1]

        elif stmt_type == 'limit':
            # statement : statement limit_statement
            check_limit_stmt(stmts, p[2][1])

            stmts['limit'] = p[2][1]
        elif stmt_type == 'group':
            check_group_stmt(stmts, p[2][1])

            stmts['group'] = p[2][1]

    check_select_stmt(stmts)


def p_select_stmt(p):
    '''
        select_statement : select_factor
                         | select_statement ','  select_factor
    '''
    if p[1][0] == 'select':
        p[0] = p[1]
    else:
        p[0] = ('select', OrderedDict())

    d = p[0][1]
    factor_idx = 1 if len(p) == 2 else 3

    t, factor = p[factor_idx]
    if t == 'alias':
        if 'alias' not in d:
            d['alias'] = {'from_alias': {}, 'to_alias': {}}
        d['alias']['from_alias'].update(factor['from_alias'])
        d['alias']['to_alias'].update(factor['to_alias'])
        t, factor = factor['factor']

    field, func = factor
    if t not in d:
        d[t] = OrderedDict()

    if field in d[t]:
        raise Exception('Duplicated fields in select, field: %s', field)

    d[t][field] = func


def p_select_factor(p):
    '''
        select_factor : a_field
                      | '*'
                      | accu_func_factor
                      | group_func_factor
                      | select_factor FNAME
    '''
    if isinstance(p[1], str):
        p[0] = ('field', (p[1], 1))
    elif len(p) == 3:
        alias = {
            'from_alias': {p[2]: p[1][1][0]},
            'to_alias': {p[1][1][0]: p[2]},
            'factor': p[1]
        }
        p[0] = ('alias', alias)
    else:
        p[0] = p[1]


def p_a_field(p):
    '''
        a_field : NAME
                | PATH
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


def p_accu_func(p):
    '''
        accu_func : AVG
                  | MAX
                  | MIN
                  | SUM
    '''
    p[0] = p[1].lower()


def p_accu_func_factor(p):
    '''
        accu_func_factor : accu_func '(' accu_field ')'
                         | COUNT '(' accu_field ')'
                         | COUNT '(' '*' ')'
    '''
    fn_idx, field_idx = 1, 3

    field = p[field_idx].lower()
    fn = p[fn_idx]

    if fn == 'sum' and field != 'size':
        raise Exception('\'sum\' can only be operated on \'size\'')

    accu_obj_name = '%s%sFuncCls' % (fn[0].upper(), fn[1:].lower())
    accu_obj = accu_func.__dict__[accu_obj_name]
    fn_key = '%s(%s)' % (fn, field)

    p[0] = ('aggregations', (fn_key, lambda: accu_obj(field)))


def p_from_stmt(p):
    'from_statement : FROM FNAME'
    p[0] = ('from', p[2])


def p_where_stmt(p):
    'where_statement : WHERE condition_statement'
    p[0] = ('where', p[2])


def p_condition_stmt1(p):
    'condition_statement : condition_statement OR and_condition'
    p1, p2 = p[1], p[3]
    p[0] = lambda finfo, alias: p1(finfo, alias) or p2(finfo, alias)


def p_condition_stmt2(p):
    'condition_statement : and_condition'
    p[0] = p[1]


def p_and_condition1(p):
    'and_condition : and_condition AND factor'
    p1, p2 = p[1], p[3]
    p[0] = lambda finfo, alias: p1(finfo, alias) and p2(finfo, alias)


def p_and_condition2(p):
    'and_condition : factor'
    p[0] = p[1]


def p_factor(p):
    '''
        factor : name_factor
               | size_factor
               | time_factor
               | alias_factor
               | '(' condition_statement ')'
               | NOT factor
    '''
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 3:
        p1 = p[2]
        p[0] = lambda finfo, alias: not p1(finfo, alias)
    elif len(p) == 4:
        p[0] = p[2]


def p_name_factor(p):
    '''
        name_factor : NAME '=' QUOTE FNAME QUOTE
                    | NAME NE QUOTE FNAME QUOTE
                    | NAME LIKE QUOTE FNAME QUOTE
    '''
    _, _, op, _, fname, _ = p
    if op == '=':
        p[0] = lambda finfo, alias: finfo['name'] == fname
    elif op == '!=':
        p[0] = lambda finfo, alias: finfo['name'] != fname
    else:
        fname = fname.replace('.', '\.')
        fname = fname.replace('%', '.*')
        pattern = re.compile(fname)
        p[0] = lambda finfo, alias: pattern.match(finfo['name']) is not None


def p_num_cmp_sub_factor(p):
    '''
        cmp_op_sub_factor : '='
                          | '>'
                          | '<'
                          | NE
                          | GE
                          | LE
    '''
    p[0] = p[1]


def p_size_factor(p):
    '''
        size_factor : SIZE cmp_op_sub_factor NUMBER
    '''
    _, _, op, fsize = p
    cmp_func = fstat_cmp_operators[op]
    p[0] = cmp_func('size', fsize)


def p_datetime_factor(p):
    '''
        datetime_factor : DATE
                        | DATE TIME
    '''
    if len(p) == 2:
        p[0] = datetime.strptime(p[1], '%Y-%m-%d')
    else:
        p[0] = datetime.strptime(p[1] + ' ' + p[2], '%Y-%m-%d %H:%M:%S')


def p_time_field(p):
    '''
        time_field : CTIME
                   | MTIME
                   | ATIME
    '''
    p[0] = p[1]


def p_time_factor(p):
    '''
        time_factor : time_field cmp_op_sub_factor datetime_factor
    '''
    time_proc(p)


def p_alias_factor(p):
    '''
        alias_factor : FNAME cmp_op_sub_factor NUMBER
                     | FNAME cmp_op_sub_factor datetime_factor
    '''
    _, f, op, val = p
    cmp_func = fstat_cmp_operators[op]
    if not isinstance(val, int):
        val = time.mktime(val.timetuple())

    p[0] = cmp_func(f, val)


def p_order_statement(p):
    '''
        order_statement : ORDER BY order_factor
                        | order_statement ',' order_factor
    '''
    if isinstance(p[1], str):
        p[0] = ('order', {'fields': OrderedDict(), 'aggregations': {}})
    else:
        p[0] = p[1]

    orders = p[0][1]['fields']

    f, ad = p[3]['field']
    if f in orders:
        raise Exception('Duplicated field of ORDER BY, field: %s' % f)
    orders[f] = ad

    if 'aggregations' in p[3]:
        p[0][1]['aggregations'].update(p[3]['aggregations'])


def p_order_sub_factor(p):
    '''
        order_sub_factor : a_field
                         | accu_func_factor
                         | group_func_factor
                         | FNAME
    '''
    # p[0]:
    #   - field: a field
    #   - aggregations: only valid when order by aggregation function
    p[0] = {}
    if isinstance(p[1], str):
        p[0]['field'] = p[1]
    else:
        t, fn = p[1]
        # fn is a tuple: (key, fn)
        p[0]['field'] = fn[0]
        if t == 'aggregations':
            p[0]['aggregations'] = {fn[0]: fn[1]}


def p_order_factor(p):
    '''
        order_factor : order_sub_factor
                     | order_sub_factor ASC
                     | order_sub_factor DESC
    '''
    # field: (field_name, asc/desc)
    p[0] = {}
    if 'aggregations' in p[1]:
        p[0]['aggregations'] = p[1]['aggregations']

    ad = 'asc' if len(p) == 2 else p[2].lower()
    p[0]['field'] = (p[1]['field'], ad)


def p_limit_statement(p):
    '''
        limit_statement : LIMIT NUMBER
                        | LIMIT NUMBER ',' NUMBER
    '''
    if not p[0]:
        p[0] = ('limit', [])

    p[0][1].append(p[2])
    if len(p) == 5:
        p[0][1].append(p[4])


def p_group_func_factor(p):
    '''
        group_func_factor : MINUTE '(' time_field ')'
                          | HOUR '(' time_field ')'
                          | DAY '(' time_field ')'
                          | MONTH '(' time_field ')'
                          | YEAR '(' time_field ')'
                          | FTYPE
    '''
    if len(p) == 5:
        k = '%s(%s)' % (p[1].lower(), p[3])
        fn = time_aggregate_operators[p[1].lower()]
        p[0] = ('dimension_aggr', (k, fn('st_' + p[3])))
    else:
        p[0] = ('dimension_aggr', ('ftype', ftype_aggregate_operator))


def p_having_statement(p):
    '''
        having_statement : HAVING having_condition
    '''
    p[0] = p[2]


def p_having_condition(p):
    '''
        having_condition : having_condition OR having_and_factor
                         | having_and_factor
    '''
    p[0] = p[1]
    if len(p) == 4:
        p[0]['aggregations'].update(p[3]['aggregations'])
        fn1, fn2 = p[1]['fn'], p[3]['fn']
        p[0]['fn'] = lambda having_data: fn1(having_data) or fn2(having_data)


def p_having_and_factor(p):
    '''
        having_and_factor : having_and_factor AND having_factor
                          | having_factor
    '''
    p[0] = p[1]
    if len(p) == 4:
        p[0]['aggregations'].update(p[3]['aggregations'])
        fn1, fn2 = p[1]['fn'], p[3]['fn']
        p[0]['fn'] = lambda having_data: fn1(having_data) and fn2(having_data)


def p_having_sub_factor(p):
    '''
        having_sub_factor : accu_func_factor
                          | FNAME
    '''
    if isinstance(p[1], str):
        p[0] = ('alias', p[1])
    else:
        p[0] = p[1]


def p_having_factor(p):
    '''
        having_factor : having_sub_factor cmp_op_sub_factor NUMBER
                      | '(' having_condition ')'
                      | NOT having_factor
    '''
    if p[1] == '(':
        # having_factor : '(' having_condition ')'
        p[0] = p[2]
    elif len(p) == 3:
        # having_factor : NOT having_factor
        p[0] = p[2]
        fn = p[0]['fn']
        p[0]['fn'] = lambda having_data: not fn(having_data)
    else:
        _, val, op, num = p

        if val[0] == 'aggregations':
            # aggregations: OrderedDict(aggregation_func_key -> AccuFuncCls)
            p[0] = {'aggregations': OrderedDict([p[1][1]])}

            # only has one key
            aggr_func_key = p[1][1][0]
        else:
            p[0] = {'aggregations': {p[1][1]: 1}}
            aggr_func_key = p[1][1]

        if op == '=':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] == num
        elif op == '!=':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] != num
        elif op == '>':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] > num
        elif op == '>=':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] >= num
        elif op == '<':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] < num
        elif op == '<=':
            p[0]['fn'] = lambda having_data: having_data[aggr_func_key] <= num


def p_group_by_statemennt(p):
    '''
        group_by_statement : GROUP BY group_func_factor
                           | GROUP BY FNAME
                           | GROUP BY group_func_factor having_statement
                           | GROUP BY FNAME having_statement
    '''
    # structure of p[0](dict, group result):
    #   'dimension_aggr'(dict: str -> func):
    #       'ftype': str fun(finfo{'name', 'stat'})
    #       'minute(ctime)': str func(finfo{'name', 'stat'})
    #       ...
    #   'having'(dict):
    #       'aggregations': OrderedDict(aggr_key -> AccuFuncCls)
    #           'max(ctime)': AccuFuncCls
    #           ...
    #       'fn': boolean func(having_data{same to 'aggregations'})
    p[0] = ('group', {})

    g = p[0][1]
    if isinstance(p[3], str):
        p[3] = ('', (p[3], 1))

    g['dimension_aggr'] = OrderedDict([p[3][1]])
    if len(p) == 5:
        g['having'] = p[4]


def time_proc(p):
    _, field_name, op, d = p
    d = time.mktime(d.timetuple())
    field_name = field_name.lower()

    cmp_func = fstat_cmp_operators[op]
    p[0] = cmp_func(field_name, d)


def p_error(p):
    if not p:
        print 'End of file'
        return
    print 'parse error, unexpected token:', p.type, p.value


parser = yacc.yacc()


if __name__ == '__main__':
    yacc.yacc()
    stmt = 'select name from . where size > 1 order by name'
    print parser.parse(stmt)
