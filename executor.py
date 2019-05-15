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


def execute_statement(stmt):
    stmts = parser.parse(stmt)
    execute(**stmts)


# def execute(s_stmt, f_stmt, w_stmt):
def execute(**kwargs):
    s_stmt = kwargs.get('select', ('select', ['*']))
    f_stmt = kwargs.get('from', '.')
    w_stmt = kwargs.get('where', lambda finfo: True)
    o_stmt = kwargs.get('order')

    # remove type of select
    s_stmt = s_stmt[1]

    show_fields = OrderedDict()
    accu_funcs = OrderedDict()
    for s in s_stmt:
        if isinstance(s, accu_func.AccuFuncCls):
            accu_funcs['_'.join(s.desp())] = s
        else:
            show_fields[s] = 1

    accu_funcs = accu_funcs.values()
    if '*' in show_fields:
        show_fields = ['*']
    else:
        show_fields = show_fields.keys()

    # print table, info for echo row
    printer = FieldPrinter(f_stmt, show_fields, accu_funcs)
    printer.print_title()

    files = []
    travel_file_tree(f_stmt, accu_funcs, w_stmt, printer, files)

    if o_stmt:
        files.sort(_order_cmp(o_stmt))

    for f in files:
        printer.print_finfo(f['name'], f['stat'])

    # print accumulative func
    printer.print_accu_funcs()


def travel_file_tree(start_point, accu_funcs, selector, printer, files):
    g = glob.glob(start_point + '/*')
    for f in g:
        statinfo = os.stat(f)
        fname = os.path.basename(f)
        finfo = {'name': fname, 'stat': statinfo}
        if selector(finfo):
            files.append(finfo)

            for accufunc in accu_funcs:
                accufunc(finfo, fname)

        if os.path.isdir(f):
            travel_file_tree(f, accu_funcs, selector, printer, files)


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

