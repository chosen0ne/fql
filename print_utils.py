#!/usr/bin/env python
# coding: utf8
#
#
# @file:    print_utils
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2015/01/21 14:43:05
import itertools
from datetime import datetime
from accu_func import AccuFuncCls


ALL_FIELDS = ['name', 'path', 'ctime', 'mtime', 'atime', 'size']


class Printer(object):

    _col_sep = '|'
    _row_sep = '-'
    _sep_line = None

    def fields(self):
        return []

    def rows(self):
        return []

    def no_border(self):
        self._col_sep, self._row_sep = '', ''

    def print_sep_line(self):
        if self._sep_line:
            print self._sep_line

    def print_table(self):
        fields = self.fields()
        rows = self.rows()

        cols_width = self._calc_cols_width(fields, rows)
        self._sep_line = self._get_sep_line(cols_width)

        # title
        if fields:
            self.print_sep_line()

            for f, w in zip(fields, cols_width):
                self._print_val(f, w)

            print self._col_sep

        if rows:
            self.print_sep_line()
            for r in rows:
                for v, w in zip(r, cols_width):
                    self._print_val(v, w)

                print self._col_sep
                self.print_sep_line()

        else:
            self.print_sep_line()

    def _get_sep_line(self, fields_len):
        # each field include '|', ' ', field, ' ' => 3
        # and the trailing '|' in each row
        padding = len(fields_len) * 3 + 1
        count = sum(fields_len)
        sep_line = ''.join(itertools.repeat(self._row_sep, count + padding))

        return sep_line

    def _calc_cols_width(self, fields, rows):
        if not fields and not rows:
            raise Exception('No fields and rows')

        if fields:
            cols_width = [len(f) for f in fields]
        else:
            cols_width = [v for v in itertools.repeat(0, len(rows[0]))]

        for r in rows:
            for idx in xrange(len(r)):
                cols_width[idx] = max(len(r[idx]), cols_width[idx])

        return cols_width

    def _fetch_size_val(self, size_val):
        unit, val = self._readable_size(size_val)
        if val - int(val) < 0.01:
            val = '%d%s' % (val, unit)
        else:
            val = '%.2f%s' % (val, unit)

        return val

    def _readable_size(self, size):
        for unit in ['B', 'K', 'M', 'G']:
            rsize = size
            size /= 1024.0
            if size <= 1:
                return unit, rsize

        return 'G', size

    def _print_val(self, val, width):
        print (self._col_sep + ' %-' + str(width) + 's') % val,


class FieldPrinter(Printer):
    def __init__(self, show_fields, files, aliases, show_border):
        if len(show_fields) == 1 and '*' in show_fields:
            self._select_fields = ALL_FIELDS
        else:
            self._select_fields = map(lambda f: f.lower(), show_fields)

        self._aliases = aliases
        self._rows = []
        for f in files:
            self._rows.append([self._fetch_val(field, f) for field in
                              self._select_fields])

        if not show_border:
            self.no_border()

    def _fetch_val(self, field, finfo):
        if field == 'name':
            return finfo['name']
        elif field == 'path':
            return finfo['path']
        else:
            f = 'st_' + field
            statinfo = finfo['stat']
            val = getattr(statinfo, f)
            if field[-4:] == 'time':
                d = datetime.fromtimestamp(val)
                return d.strftime('%Y-%m-%d %H:%M:%S')
            elif field == 'size':
                return self._fetch_size_val(val)

    def fields(self):
        a = self._aliases
        fields = map(lambda f: a['to_alias'][f] if a and f in a['to_alias']
                     else f, self._select_fields)
        return fields

    def rows(self):
        return self._rows


class AggregatePrinter(Printer):
    def __init__(self, from_dir, accu_funcs, aliases, show_border):
        self._rows = []
        for fn in accu_funcs.values():
            f = aliases['to_alias'][fn.key()] if aliases and \
                fn.key() in aliases['to_alias'] else fn.key()
            field_name = '%s of %s' % (f, from_dir)
            if fn.fname():
                field_name += ': ' + fn.fname()

            val = self._fetch_size_val(fn.val()) if fn.desp()[1] == 'size' \
                else fn.val()

            self._rows.append((field_name, str(val)))

        if not show_border:
            self.no_border()

    def rows(self):
        return self._rows


class GroupPrinter(Printer):
    def __init__(self, dim_rows, dim_name, accu_fns, aliases, show_border):
        self._fields = [dim_name]
        self._fields.extend([f().key() for f in accu_fns.values()])
        self._aliases = aliases

        self._rows = []
        # dim_rows is a list of dict{str -> str / AccuFuncCls}
        for r_dict in dim_rows:
            r = []
            for f in self._fields:
                val = r_dict[f]
                if isinstance(val, AccuFuncCls):
                    fn = val
                    t, f = fn.desp()
                    val = self._fetch_size_val(fn.val()) if f == 'size' else \
                        str(fn.val())
                    if fn.fname():
                        val = val + ': ' + fn.fname()

                r.append(val)

            self._rows.append(r)

        if not show_border:
            self.no_border()

    def fields(self):
        a = self._aliases
        fields = map(lambda f: a['to_alias'][f] if a and f in a['to_alias']
                     else f, self._fields)
        return fields

    def rows(self):
        return self._rows
