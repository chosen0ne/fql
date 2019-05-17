#!/usr/bin/env python
# coding=utf8
#
#
# @file:    executor
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2019-05-15 16:53:45

import os.path
import glob
import os
import accu_func
from collections import OrderedDict
from print_utils import FieldPrinter
from grammar_parser import parser
from groupby import GroupBy


func_type = type(lambda a: 0)
MODE_SELECT_FIELDS = 1
MODE_SELECT_AGGR = 2
MODE_GROUP_AGGR = 3


def execute_statement(stmt):
    stmts = parser.parse(stmt)
    execute(**stmts)

# key word parameters:
#   - select(dict{str -> object}): select the fields to be return
#       - 'field' -> list of str: fields will be selected
#       - 'aggregate' -> OrderedDict{str -> AccuFuncCls func()}
#           - aggregation name on field -> aggregation function creator
#               - max(size) -> lambda
#       - 'dimension_aggr' -> OrderedDict{str -> str func(finfo{'name', 'stat'})}
#           - dimension aggregation name on field -> dimension fetch function
#               - minute(atime) -> lambda / ftype -> lambda
#   - from(str): directory to be query
#   - where(boolean func(finfo{'name', 'stat'})): filter files base on name or file stats
#   - order(OrderedDict{str -> str}): sort the result
#       - field name -> 'asc' or 'desc'
#   - limit(list of int): limit the count of result
#       - [limit]
#       - [limit, start]
#   - group(dict{str -> OrderedDict}): group result by some dimensions
#       - 'dimension_aggr' -> OrderedDict{str -> str func(finfo{'name', 'stat'})}
#           - minute(atime) -> lambda / ftype -> lambda
#       - 'having' -> dict{str -> object}
#           - aggregations -> OrderedDict{str -> str func(finfo{'name', 'stat'})}
#               - having on the aggregation function on fields
#           - fn -> boolean func(dict{str -> val})
#               - str -> val => aggregation function on field -> number
#                   - max(size) -> 100
def execute(**kwargs):
    s_stmt = kwargs.get('select', ('select', ['*']))
    f_stmt = kwargs.get('from', '.')
    w_stmt = kwargs.get('where', lambda finfo: True)
    o_stmt = kwargs.get('order')
    l_stmt = kwargs.get('limit')
    g_stmt = kwargs.get('group')

    show_fields = set([f for f in s_stmt['field']]) if 'field' in s_stmt else {}
    accu_funcs = s_stmt['aggregate'] if 'aggregate' in s_stmt else {}
    dim_fields = '&'.join([k for k in s_stmt['dimension_aggr'].keys()]) \
            if 'dimension_aggr' in s_stmt else None

    if show_fields:
        query_mode = MODE_SELECT_FIELDS
    else if not g_stmt:
        query_mode = MODE_SELECT_AGGR
    else:
        query_mode = MODE_GROUP_AGGR

    # use GroupBy to process group by aggragation or normal aggragation.
    # When normal aggragation is executed, all the files is treated as in one group '*'
    if query_mode == MODE_GROUP_AGGR:
        # no group by, all the files are in one group
        g_stmt = {'dimension_aggr': OrderedDict({'*': lambda a: '*'})}

    g_stmt['accu_funcs'] = accu_funcs

    groupby = GroupBy(**g_stmt)

    # print table, info for echo row
    printer = FieldPrinter(f_stmt, show_fields, dim_fields, groupby)
    printer.print_title()

    # all the files matched to where condition
    files = []
    travel_file_tree(f_stmt, w_stmt, printer, files, groupby)

    if o_stmt:
        files.sort(_order_cmp(o_stmt))

    if l_stmt:
        if len(l_stmt) == 1:
            files = files[0:l_stmt[0]]
        else:
            count, start = l_stmt
            files = files[start: start + count]

    for f in files:
        printer.print_finfo(f['name'], f['stat'])

    # print accumulative func
    printer.print_accu_funcs()

    printer.print_group_by()

# @param start_point(str)
# @param selector(func: boolean selector(finfo))
# @param printer(FieldPrinter)
# @param files(list of finfo{'name', 'stat'})
def travel_file_tree(start_point, selector, printer, files, groupby=None):
    g = glob.glob(start_point + '/*')
    for f in g:
        statinfo = os.stat(f)
        fname = os.path.basename(f)
        finfo = {'name': fname, 'stat': statinfo}
        if selector(finfo):
            files.append(finfo)

            groupby(finfo)

        if os.path.isdir(f):
            travel_file_tree(f, selector, printer, files, groupby)


def _order_cmp(order_keys):
    def inner_cmp(a, b):
        # type of a, b is (filename, fstat)
        for k, ad in reversed(order_keys.items()):
            if k == 'name':
                if a['name'] == b['name']:
                    continue
                return cmp(a['name'], b['name']) if ad == 'asc' else cmp(b['name'], a['name'])
            else:
                vala = int(getattr(a['stat'], 'st_' + k))
                valb = int(getattr(b['stat'], 'st_' + k))
                if vala == valb:
                    continue
                return cmp(vala, valb) if ad == 'asc' else cmp(valb, vala)

        return 0

    return inner_cmp

