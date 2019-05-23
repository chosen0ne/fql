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
import json
from collections import OrderedDict
from print_utils import FieldPrinter, AggregatePrinter, GroupPrinter
from grammar_parser import parser
from groupby import GroupBy
from accu_func import AccuFuncCls


func_type = type(lambda a: 0)
MODE_SELECT_FIELDS = 1
MODE_SELECT_AGGR = 2
MODE_GROUP_AGGR = 3


def execute_statement(stmt, conf={}):
    stmts = parser.parse(stmt)
    if stmts is None:
        raise Exception('failed to parse, statement: %s' % stmt)

    stmts.update(conf)
    execute(**stmts)


def execute(**kwargs):
    '''
    key word parameters:
    - select(dict{str -> object}): select the fields to be return
        - 'field' -> list of str: fields will be selected
        - 'aggregations' -> OrderedDict{str -> AccuFuncCls func()}
            - aggregation name on field -> aggregation function creator
                - max(size) -> lambda
        - 'dimension_aggr' -> OrderedDict{str ->
           str func(finfo{'name', 'stat'})}
            - dimension aggregation name on field -> dimension fetch function
                - minute(atime) -> lambda / ftype -> lambda
    - from(str): directory to be query
    - where(boolean func(finfo{'name', 'stat'})): filter files base on name or
      file stats
    - order(OrderedDict{str -> str}): sort the result
        - field name -> 'asc' or 'desc'
    - limit(list of int): limit the count of result
        - [limit]
        - [limit, start]
    - group(dict{str -> OrderedDict}): group result by some dimensions
        - 'dimension_aggr' -> OrderedDict{str ->
          str func(finfo{'name', 'stat'})}
            - minute(atime) -> lambda / ftype -> lambda
        - 'having' -> dict{str -> object}
            - aggregations -> OrderedDict{str ->
              str func(finfo{'name', 'stat'})}
                - having on the aggregation function on fields
            - fn -> boolean func(dict{str -> val})
                - str -> val => aggregation function on field -> number
                    - max(size) -> 100
    '''
    s_stmt = kwargs.get('select', ('select', ['*']))
    f_stmt = kwargs.get('from', '.')
    w_stmt = kwargs.get('where', lambda finfo, alias: True)
    o_stmt = kwargs.get('order')
    l_stmt = kwargs.get('limit')
    g_stmt = kwargs.get('group')

    is_debug = kwargs.get('debug')
    max_depth = kwargs.get('depth')

    if is_debug:
        o = json.dumps(kwargs, indent=4, separators=(',', ':'),
                       cls=OuputJsonEncoder)
        print 'kwargs:', o

    show_fields = set([f for f in s_stmt['field']]) if 'field' in s_stmt else \
        set()
    accu_funcs = s_stmt['aggregations'] if 'aggregations' in s_stmt else {}
    dim_fields = '&'.join([k for k in s_stmt['dimension_aggr'].keys()]) \
        if 'dimension_aggr' in s_stmt else None
    aliases = s_stmt['alias'] if 'alias' in s_stmt else None

    # replacement of alias
    if aliases:
        if o_stmt:
            order_fields = o_stmt['fields']
            alias_replace(aliases['from_alias'], order_fields)

        if g_stmt:
            group_fields = g_stmt['dimension_aggr']
            aggregation_alias_replace(aliases['from_alias'], group_fields,
                                      s_stmt['dimension_aggr'])

            if 'having' in g_stmt:
                having_fields = g_stmt['having']['aggregations']
                aggregation_alias_replace(aliases['from_alias'], having_fields,
                                          s_stmt['aggregations'])

    if show_fields:
        query_mode = MODE_SELECT_FIELDS
    elif not g_stmt:
        query_mode = MODE_SELECT_AGGR
    else:
        query_mode = MODE_GROUP_AGGR

    if query_mode == MODE_SELECT_AGGR and o_stmt:
        raise Exception('\'order by\' isn\'t supported in select aggregation')

    if query_mode == MODE_SELECT_AGGR and l_stmt:
        raise Exception('\'limit\' isn\'t supported in select aggregation')

    # use GroupBy to process group by aggragation or normal aggragation.
    # When normal aggragation is executed, all the files is treated as in one
    # group '*'
    if query_mode != MODE_GROUP_AGGR:
        # no group by, all the files are in one group
        g_stmt = {'dimension_aggr': OrderedDict({'*': lambda a: '*'})}

    g_stmt['accu_funcs'] = accu_funcs
    g_stmt['order_accu_funcs'] = o_stmt['aggregations'] if o_stmt else None
    g_stmt['aliases'] = aliases

    if is_debug:
        p = {
            'select': s_stmt,
            'order': o_stmt,
            'l_stmt': l_stmt,
            'group': g_stmt,
            'where': w_stmt
        }
        o = json.dumps(p, indent=4, separators=(',', ':'),
                       cls=OuputJsonEncoder)
        print 'kwargs processed: ', o

    groupby = GroupBy(**g_stmt)
    if query_mode == MODE_GROUP_AGGR and dim_fields != groupby.get_dim_name():
        raise Exception('Dimensions in select and group by are different, '
                        'select: %s, group by: %s'
                        % (dim_fields, groupby.get_dim_name()))

    # all the files matched to where condition
    files = []
    travel_file_tree(f_stmt, w_stmt, files, groupby, 1, max_depth)

    # fetch rows
    order_fn = None
    if query_mode == MODE_SELECT_FIELDS:
        rows = files
        if o_stmt:
            order_fn = _fields_order_cmp
    elif query_mode == MODE_SELECT_AGGR:
        rows = groupby.get_dimension_vals()['*']
    else:
        rows = groupby.get_dimension_rows()
        if o_stmt:
            order_fn = _group_order_cmp

    if order_fn:
        rows.sort(order_fn(o_stmt['fields']))

    if query_mode != MODE_SELECT_AGGR and l_stmt:
        s, c = (0, l_stmt[0]) if len(l_stmt) == 1 else l_stmt
        rows = rows[s: s+c]

    if query_mode == MODE_SELECT_FIELDS:
        printer = FieldPrinter(show_fields, rows, aliases)
    elif query_mode == MODE_SELECT_AGGR:
        printer = AggregatePrinter(f_stmt, rows, aliases)
    elif query_mode == MODE_GROUP_AGGR:
        printer = GroupPrinter(rows, groupby.get_dim_name(), accu_funcs,
                               aliases)

    printer.print_table()


# @param start_point(str)
# @param selector(func: boolean selector(finfo))
# @param printer(FieldPrinter)
# @param files(list of finfo{'name', 'stat'})
def travel_file_tree(start_point, selector, files, groupby, cur_depth=1,
                     max_depth=3):
    if cur_depth > max_depth:
        return

    g = glob.glob(start_point + '/*')
    for f in g:
        statinfo = os.stat(f)
        fname = os.path.basename(f)
        finfo = {'name': fname, 'stat': statinfo}
        if selector(finfo, groupby.get_aliases()):
            files.append(finfo)

            groupby(finfo)

        if os.path.isdir(f):
            travel_file_tree(f, selector, files, groupby, cur_depth+1,
                             max_depth)


def _fields_order_cmp(order_keys):
    def inner_cmp(a, b):
        # type of a, b is (filename, fstat)
        for k, ad in order_keys.items():
            if k == 'name':
                if a['name'] == b['name']:
                    continue
                return cmp(a['name'], b['name']) if ad == 'asc' else \
                    cmp(b['name'], a['name'])
            else:
                vala = int(getattr(a['stat'], 'st_' + k))
                valb = int(getattr(b['stat'], 'st_' + k))
                if vala == valb:
                    continue
                return cmp(vala, valb) if ad == 'asc' else cmp(valb, vala)

        return 0

    return inner_cmp


def _group_order_cmp(order_keys):
    def inner_cmp(a, b):
        '''
            a & b is a dict{str -> object}
        '''
        for k, ad in order_keys.items():
            va, vb = a[k], b[k]
            if isinstance(a[k], AccuFuncCls):
                va, vb = a[k].val(), b[k].val()
            if va == vb:
                continue

            return cmp(va, vb) if ad == 'asc' else cmp(vb, va)

        return 0

    return inner_cmp


def alias_replace(aliases, data_dict):
    for f, data in data_dict.items():
        if f in aliases:
            data_dict[aliases[f]] = data
            del(data_dict[f])


def aggregation_alias_replace(aliases, data_dict, aggr_funcs):
    for f, data in data_dict.items():
        if f in aliases:
            dim_name = aliases[f]
            if not aggr_funcs or dim_name not in aggr_funcs:
                raise Exception('undefined aggregation alias for %s' % f)
            data_dict[dim_name] = aggr_funcs[dim_name]
            del(data_dict[f])


class OuputJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, type(_group_order_cmp)):
            return '%s.%s' % (obj.__module__, obj.func_name)
        return json.JSONEncoder.default(self, obj)
