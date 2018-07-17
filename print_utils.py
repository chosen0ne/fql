#!/bin/env python
# coding: utf8
#
#
# @file:    print_utils
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2015/01/21 14:43:05
import itertools
from datetime import datetime

allfields = ['name', 'ctime', 'mtime', 'atime', 'size']

fields_info = {
    'name': 20,
    'ctime': 20,
    'mtime': 20,
    'atime': 20,
    'size': 10,
}


class FieldPrinter(object):
    def __init__(self, fields, accufuncs):
        if len(fields) == 1 and '*' in fields:
            select_fields_info = fields_info
            self._select_fields = allfields
        else:
            self._select_fields = map(lambda x: x.lower(), fields)
            select_fields_info = {}
            for f in self._select_fields:
                select_fields_info[f] = fields_info[f]

        self._accufuncs = accufuncs
        self._fsep_line = self._get_sep_line(select_fields_info.values())

        # calculate each column width for accumulative functions
        self._field_col = reduce(lambda x, y: x if x > y else y,
                                 map(lambda x: len(x.desp()[1]), accufuncs), 0)
        self._field_col += 5 + 4
        self._val_col = reduce(lambda x, y: x if x > y else y,
                               fields_info.values(), 0)
        self._asep_line = self._get_sep_line([self._field_col, self._val_col])

    def print_title(self):
        if not self._select_fields:
            return

        print self._fsep_line
        for f in self._select_fields:
            col_len = fields_info[f]
            print ('| %-' + str(col_len - 1) + 's') % f,

        print '|'
        print self._fsep_line

    def print_finfo(self, fname, statinfo):
        if not self._select_fields:
            return

        for f in self._select_fields:
            col_len = fields_info[f]
            fc, val = self._fetch_val(f, fname, statinfo)
            print ('| %-' + str(col_len - 1) + fc) % val,
        print '|'
        print self._fsep_line

    def print_accu_funcs(self):
        if not self._accufuncs:
            return

        print self._asep_line
        for func in self._accufuncs:
            field_name = '%s(%s)' % func.desp()
            if func.fname() is not None:
                field_name += ': %s' % func.fname()
            print ('| %-' + str(self._field_col - 1) + 's') % field_name,

            if func.desp()[1] == 'st_size':
                unit, val = self._readable_size(func.val())
                if val - int(val) < 0.01:
                    val = '%d%s' % (val, unit)
                else:
                    val = '%.2f%s' % (val, unit)
            else:
                val = func.val()
            print ('| %-' + str(self._val_col - 1) + 's') % val,
            print '|'
            print self._asep_line

    def _get_sep_line(self, fields_len):
        padding = len(fields_len) * 2 + 1
        count = reduce(lambda x, y: x + y, fields_len, 0)
        sep_line = ''.join(itertools.repeat('-', count + padding))

        return sep_line

    def _fetch_val(self, field, fname, statinfo):
        if field == 'name':
            return 's', fname
        else:
            f = 'st_' + field
            val = getattr(statinfo, f)
            if field[-4:] == 'time':
                d = datetime.fromtimestamp(val)
                return 's', d.strftime('%Y-%m-%d %H:%M:%S')
            elif field == 'size':
                unit, size = self._readable_size(val)
                if size - int(size) < 0.01:
                    return 's', '%d%s' % (int(size), unit)
                return 's', ('%.2f' % size) + unit

    def _readable_size(self, size):
        for unit in ['B', 'K', 'M', 'G']:
            rsize = size
            size /= 1024.0
            if size <= 1:
                return unit, rsize

        return 'G', size


if __name__ == '__main__':
    p = FieldPrinter(['name', 'ctime', 'size'])
    p.print_title()
    p = FieldPrinter(['*'])
    p.print_title()
