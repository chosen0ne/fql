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

    select_statement : fields_select_stmt
                     | accu_func_stmt
                     | accu_func_stmt ',' group_func_factor
                     | group_func_factor ',' accu_func_stmt

    from_statement : FROM FNAME

    where_statement : WHERE condition_statement

    order_statement : ORDER BY order_factor
                    | order_statement, order_factor

    limit_statement : LIMIT NUMBER
                    | LIMIT NUMBER ',' NUMBER

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

    order_sub_factor : a_field
                     | accu_func_stmt
                     | group_func_factor

    order_factor : order_sub_factor
                 | order_sub_factor ASC
                 | order_sub_factor DESC

    time_factor : ATIME
                | CTIME
                | MTIME

    group_func_factor : MINUTE '(' time_factor ')'
                      | HOUR '(' time_factor ')'
                      | DAY '(' time_factor ')'
                      | MONTH '(' time_factor ')'
                      | YEAR '(' time_factor ')'
                      | FTYPE

    having_statement : HAVING having_condition

    having_condition : having_condition OR having_and_factor
                     | having_and_factor

    having_and_factor : having_and_factor AND having_factor
                      | having_factor

    having_factor : accu_func_stmt '=' NUMBER
                  | accu_func_stmt NE NUMBER
                  | accu_func_stmt '>' NUMBER
                  | accu_func_stmt GE NUMBER
                  | accu_func_stmt '<' NUMBER
                  | accu_func_stmt LE NUMBER
                  | '(' having_condition ')'
                  | NOT having_factor

    group_by_statement : GROUP BY group_func_factor
                       | GROUP BY group_func_factor having_statement

'''

# precedence = (
#         ('left', 'NUMBER'),
#         ('left', ',')
# )

# used to compare file stats, such as st_size, st_ctime, st_atime...
fstat_cmp_operators = {
        '=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) == val,
        '>': lambda field, val: lambda finfo: getattr(finfo['stat'], field) > val,
        '<': lambda field, val: lambda finfo: getattr(finfo['stat'], field) < val,
        '!=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) != val,
        '>=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) >= val,
        '<=': lambda field, val: lambda finfo: getattr(finfo['stat'], field) <= val
}


time_aggregate_operators = {
        'minute': lambda field: lambda finfo: datetime.fromtimestamp(getattr(finfo['stat'], field))
                .strftime('%Y-%m-%d %H:%M'),
        'hour': lambda field: lambda finfo: datetime.fromtimestamp(getattr(finfo['stat'], field))
                .strftime('%Y-%m-%d %H'),
        'day': lambda field: lambda finfo: datetime.fromtimestamp(getattr(finfo['stat'], field))
                .strftime('%Y-%m-%d'),
        'month': lambda field: lambda finfo: datetime.fromtimestamp(getattr(finfo['stat'], field))
                .strftime('%Y-%m'),
        'year': lambda field: lambda finfo: datetime.fromtimestamp(getattr(finfo['stat'], field))
                .strftime('%Y'),
}


# fetch file type '.*$'
def ftype_aggregate_operator(finfo):
    idx = finfo['name'].rfind('.')
    return finfo['name'][idx:] if idx != -1 else '$'


def check_order_stmt(stmts, order_stmt):
    if 'order' in stmts:
        raise Exception('Duplicated order by, exists: order by %s, here: order by %s' %
                        (stmts['order'].items(), order_stmt.items()))

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
        stmts['select'], stmts['from'], stmts['where'] = p[2][1], p[3][1], p[4][1]

    elif len(p) == 4:
        stmt_type, stmt = p[2]
        if stmt_type == 'select':
            stmts['select'] = p[2][1]
            stmts[p[3][0]] = p[3][1]
        elif stmt_type == 'from':
            stmts['from'], stmts['where'] = p[2][1], p[3][1]

    elif len(p) == 3:
        stmt_type, stmt = p[2]
        if stmt_type == 'where' or stmt_type == 'select' or stmt_type == 'from':
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


def p_select_stmt(p):
    '''
        select_statement : fields_select_stmt
                         | accu_func_stmt
                         | accu_func_stmt ',' group_func_factor
                         | group_func_factor ',' accu_func_stmt
    '''
    # format of p[0]: ('select', ('field', [])), ('select', ('accu', [])), ('group_aggr', {'key', 'fn'})
    p[0] = ('select', {})

    d = p[0][1]
    d[p[1][0]] = p[1][1]

    if len(p) == 4:
        d[p[3][0]] = p[3][1]
    # aggr_func_idx = 1 if p[1][0] == 'aggregate' else 3
    # # format of aggregation functions:
    # #   ('aggregate', OrderedDict([(max_st_size, MaxAccFuncCls), (min_st_ctime, MinAccFuncCls)])
    # #       transmit to
    # #   ('aggregate', [MaxAccFuncCls, MinAccFuncCls])
    # aggr_funcs = [v for _, v in p[aggr_func_idx][1].items()]

    # if len(p) == 2:
    #     p[0] = ('select', p[1])

    # if len(p) == 4:
    #     if p[1][0] == 'group_aggr':
    #         aggr_funcs.insert(0, p[1][1]['key'])
    #     else:
    #         aggr_funcs.append(p[1][1]['key'])

    # p[0] = ('select', aggr_funcs)


def p_fields_select_stmt(p):
    '''
        fields_select_stmt : fields_select_stmt ','  a_field
                           | a_field
                           | '*'
    '''
    if isinstance(p[1], tuple):
        # fields_select_stmt : fields_select_stmt ',' a_field
        p[0] = p[1]
    else:
        p[0] = ('field', [])

    fields = p[0][1]
    if len(p) == 4:
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
    f = p[3].lower()
    op = p[1]
    accu_obj_name = op[0].upper() + op[1:].lower() + 'FuncCls'
    accu_obj = accu_func.__dict__[accu_obj_name]
    func_key = '%s(%s)' % (op, p[3])
    # fns = OrderedDict([(func_key.lower(), accu_obj(f))])
    fns = OrderedDict([(func_key.lower(), lambda: accu_obj(f))])
    p[0] = ('aggregate', fns)


def p_accu_func_stmt2(p):
    '''
        accu_func_stmt : accu_func_stmt ',' AVG '(' accu_field ')'
                       | accu_func_stmt ',' MAX '(' accu_field ')'
                       | accu_func_stmt ',' MIN '(' accu_field ')'
                       | accu_func_stmt ',' COUNT '(' '*' ')'
                       | accu_func_stmt ',' SUM '(' SIZE ')'
    '''

    p[0] = p[1]

    fns = p[0][1]
    f = p[5].lower()
    op = p[3]
    accu_obj_name = op[0].upper() + op[1:].lower() + 'FuncCls'
    accu_obj = accu_func.__dict__[accu_obj_name]

    func_key = '%s(%s)' % (op, p[5])
    if func_key in fns:
        raise Exception('Duplicated aggregation function, func: %s', func_key)

    fns[func_key] = lambda: accu_obj(f)


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
    cmp_func = fstat_cmp_operators[op]
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
    if isinstance(p[1], tuple):
        p[0] = p[1]
    else:
        p[0] = ('order', OrderedDict())

    orders = p[0][1]

    for f, ad in p[3]:
        if f in orders:
            raise Exception('Duplicated field of ORDER BY, field: %s' % f)
        orders[f] = ad


def p_order_sub_factor(p):
    '''
        order_sub_factor : a_field
                         | accu_func_stmt
                         | group_func_factor
    '''
    # return a list of fields
    if isinstance(p[1], str):
        p[0] = [p[1]]
    else:
        p[0] = p[1][1].keys()


def p_order_factor(p):
    '''
        order_factor : order_sub_factor
                     | order_sub_factor ASC
                     | order_sub_factor DESC
    '''
    # [(field_name, asc/desc)*]
    p[0] = []
    if len(p) == 2:
        p[0].extend([(f, 'asc') for f in p[1]])
    else:
        # 'max(ctime), count(*) asc' will be pared to
        #       order_sub_factor ASC
        # p[1] = ['max(ctime)', 'count(*)']
        # ASC or DESC need to be added to the last element
        p[0].extend([(f, 'asc') for f in p[1][:-1]])
        p[0].append((p[1][-1], p[2].lower() if len(p) == 3 else 'asc'))


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


def p_time_factor(p):
    '''
        time_factor : ATIME
                    | CTIME
                    | MTIME
    '''
    p[0] = p[1]


def p_group_func_factor(p):
    '''
        group_func_factor : MINUTE '(' time_factor ')'
                          | HOUR '(' time_factor ')'
                          | DAY '(' time_factor ')'
                          | MONTH '(' time_factor ')'
                          | YEAR '(' time_factor ')'
                          | FTYPE
    '''
    if len(p) == 5:
        k = '%s(%s)' % (p[1].lower(), p[3])
        fn = time_aggregate_operators[p[1].lower()]
        p[0] = ('dimension_aggr', OrderedDict({k: fn('st_' + p[3])}))
    else:
        p[0] = ('dimension_aggr', OrderedDict({'ftype': ftype_aggregate_operator}))


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
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[1]['aggregations'].update(p[3]['aggregations'])
        p[0] = {'aggregations': p[1],
                'fn': lambda having_data: p[1]['fn'](having_data) or p[3]['fn'](having_data)}


def p_having_and_factor(p):
    '''
        having_and_factor : having_and_factor AND having_factor
                          | having_factor
    '''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[1]['aggregations'].update(p[3]['aggregations'])
        p[0] = {'aggregations': p[1],
                'fn': lambda having_data: p[1]['fn'](having_data) and p[3]['fn'](having_data)}


def p_having_factor(p):
    '''
        having_factor : accu_func_stmt '=' NUMBER
                      | accu_func_stmt NE NUMBER
                      | accu_func_stmt '>' NUMBER
                      | accu_func_stmt GE NUMBER
                      | accu_func_stmt '<' NUMBER
                      | accu_func_stmt LE NUMBER
                      | '(' having_condition ')'
                      | NOT having_factor
    '''
    if p[1] == '(':
        # having_factor : '(' having_condition ')'
        p[0] = p[2]
    elif len(p) == 3:
        # having_factor : NOT having_factor
        p[0] = p[2]
        p[0]['fn'] = lambda having_data: not p[0]['fn']
    else:
        _, val, op, num = p
        if len(val[1]) != 1:
            raise Exception('Only 1 aggregation function is supported for comparision, funcs:',
                            [v for v, _ in val[1].items()])

        # aggregations: OrderedDict(aggregation_func_key -> AccuFuncCls)
        p[0] = {'aggregations': p[1][1]}

        # only has one key
        aggr_func_key = p[1][1].keys()[0]

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
                           | GROUP BY group_func_factor having_statement
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
    g['dimension_aggr'] = p[3][1]
    if len(p) == 5:
        g['having'] = p[4]


def time_proc(p):
    _, field_name, op, d = p
    d = time.mktime(d.timetuple())
    field_name = 'st_' + field_name.lower()

    cmp_func = fstat_cmp_operators[op]
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
