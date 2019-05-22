#!/usr/bin/env python
# coding=utf8
#
#
# @file:    groupby
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2019-05-16 11:04:32

from collections import OrderedDict


class GroupBy(object):
    def __init__(self, **kwargs):
        accu_funcs = kwargs.get('accu_funcs', {})
        having = kwargs.get('having')
        # dict: dimension name -> str func(finfo{'name', {'stat'}})
        # support multiple dimensions
        self._dimensions = kwargs.get('dimension_aggr')
        self._aliases = kwargs.get('aliases')
        order_accu_funcs = kwargs.get('order_accu_funcs', {})

        # list of AccuFuncCls
        self._accu_func_creators = accu_funcs.values()

        if having:
            self._accu_func_creators.extend(having['aggregations'].values())
            selector = having['fn']
        else:
            selector = None

        if order_accu_funcs:
            self._accu_func_creators.extend(order_accu_funcs.values())

        # func: boolean func(accu_result)
        # accu_result is value of aggregations
        self._accu_selector = selector

        # OrderedDict: dimension(str) -> dict{aggr func key -> AccuFuncCls}
        self._dimension_accufuncs = OrderedDict()

        self._dim_name = '&'.join([n for n in self._dimensions.keys()])

    def __call__(self, finfo):
        dim_val = '&'.join([d(finfo) for d in self._dimensions.values()])

        dim_val.strip()
        if dim_val not in self._dimension_accufuncs:
            self._dimension_accufuncs[dim_val] = OrderedDict()
            for f in self._accu_func_creators:
                fn = f()
                self._dimension_accufuncs[dim_val][fn.key()] = fn

        for f in self._dimension_accufuncs[dim_val].values():
            f(finfo)

    def get_dimension_vals(self):
        if self._accu_selector is None:
            return self._dimension_accufuncs

        ret = OrderedDict()
        for d, acc_vals_row in self._dimension_accufuncs.items():
            # add aliases
            if self._aliases:
                for k, acc_fn in acc_vals_row.items():
                    if k in self._aliases['to_alias']:
                        acc_vals_row[self._aliases['to_alias'][k]] = acc_fn

            if not self._accu_selector or \
                    self._accu_selector(dict([(k, fn.val()) for k, fn in
                                              acc_vals_row.items()])):
                ret[d] = acc_vals_row

        return ret

    def get_dimension_rows(self):
        '''
            return list of dict{aggre func key -> AccuFuncCls}
        '''
        rows = []
        for d, acc_vals_row in self._dimension_accufuncs.items():
            # add aliases
            if self._aliases:
                for k, acc_fn in acc_vals_row.items():
                    if k in self._aliases['to_alias']:
                        acc_vals_row[self._aliases['to_alias'][k]] = acc_fn

            if not self._accu_selector or \
                    self._accu_selector(dict([(k, fn.val()) for k, fn in
                                              acc_vals_row.items()])):
                acc_vals_row[self._dim_name] = d
                rows.append(acc_vals_row)

        return rows

    def get_accu_func(self):
        return [f() for f in self._accu_func_creators]

    def get_dim_name(self):
        return self._dim_name

    def get_aliases(self):
        return self._aliases
