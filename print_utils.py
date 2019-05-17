#!/usr/bin/env python
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
    def __init__(self, from_dir, fields, dim_fields, groupby):
        if len(fields) == 1 and '*' in fields:
            select_fields_info = fields_info
            self._select_fields = allfields
        else:
            self._select_fields = map(lambda x: x.lower(), fields)
            select_fields_info = {}
            for f in self._select_fields:
                select_fields_info[f] = fields_info[f]

        self._accufuncs = groupby.get_accu_func()
        self._from_dir = from_dir
        self._groupby = groupby
        self._fsep_line = self._get_sep_line(select_fields_info.values())

        # calculate each column width for accumulative functions
        # min and max function whill show the file path, so increase the column length
        acc_func_lens = map(lambda x: len(x.desp()[1]) + 20 if x.desp()[0] in ['min', 'max'] else 0,
                            self._accufuncs)
        self._field_col = reduce(lambda x, y: x if x > y else y, acc_func_lens, 0)
        self._field_col += 5 + 4
        self._val_col = reduce(lambda x, y: x if x > y else y,
                               fields_info.values(), 0)
        self._asep_line = self._get_sep_line([self._field_col, self._val_col])

        # calculate column width for group by
        self._group_val_col = max(self._field_col, self._val_col)
        cols = [v for v in itertools.repeat(self._group_val_col, self._groupby.get_accu_func_count())]
        if dim_fields:
            cols.append(self._group_val_col)
        self._gsep_line = self._get_sep_line(cols)
        self._group_col_fmt = '| %-' + str(self._group_val_col - 1) + 's'

    def is_select_agg(self):
        return not self._select_fields and not self._groupby.is_groupby()

    def print_title(self):
        # only for select field and group by
        if self.is_select_agg():
            return

        if not self._groupby.is_groupby():
            # select field
            print self._fsep_line
            for f in self._select_fields:
                col_len = fields_info[f]
                print ('| %-' + str(col_len - 1) + 's') % f,

            print '|'
            print self._fsep_line

        else:
            # group by
            print self._gsep_line
            print self._group_col_fmt % self._groupby.get_dim_name(),
            for f in self._accufuncs:
                print self._group_col_fmt % f.key(),

            print '|'
            print self._gsep_line


    def print_finfo(self, fname, statinfo):
        if not self._select_fields:
            return

        for f in self._select_fields:
            col_len = fields_info[f]
            fc, val = self._fetch_val(f, fname, statinfo)
            print ('| %-' + str(col_len - 1) + fc) % val,
        print '|'
        print self._fsep_line

    def _fetch_size_val(self, size_val):
        unit, val = self._readable_size(size_val)
        if val - int(val) < 0.01:
            val = '%d%s' % (val, unit)
        else:
            val = '%.2f%s' % (val, unit)

        return val

    def print_accu_funcs(self):
        if not self.is_select_agg():
            return

        print self._asep_line
        accufuncs = self._groupby.get_dimension_vals()['*']
        for func in accufuncs:
            field_name = func.key() + ' of %s' % self._from_dir
            if func.fname() is not None:
                field_name += ': %s' % func.fname()
            print ('| %-' + str(self._field_col - 1) + 's') % field_name,

            val = self._fetch_size_val(func.val()) if func.desp()[1] == 'st_size' else func.val()
            print ('| %-' + str(self._val_col - 1) + 's') % val,
            print '|'
            print self._asep_line

    def print_group_by(self):
        if not self._groupby.is_groupby():
            return

        dim_vals = self._groupby.get_dimension_vals()
        for dim, funcs in dim_vals.items():
            print self._group_col_fmt % dim,
            for f in funcs:
                val = self._fetch_size_val(f.val()) if f.desp()[1] == 'st_size' else f.val()
                if f.desp()[0] in ['min', 'max']:
                    val = f.fname() + ': ' + val
                print self._group_col_fmt % val,

            print '|'
            print self._gsep_line

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
