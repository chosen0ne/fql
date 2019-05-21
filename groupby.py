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

        if having:
            having_aggr = having['aggregations']
            for k, v in having_aggr.items():
                if not k in accu_funcs:
                    accu_funcs[k] = v
            selector = having['fn']
        else:
            selector = None

        # list of AccuFuncCls
        self._accu_func_creators = accu_funcs.values()
        # func: boolean func(accu_result)
        # accu_result is value of aggregations
        self._accu_selector = selector

        # OrderedDict: dimension(str) -> dict{aggr func key -> AccuFuncCls}
        self._dimension_accufuncs = OrderedDict()

        self._dim_name = '&'.join([n for n in self._dimensions.keys()])

    def __call__(self, finfo):
        dim_val = '&'.join([d(finfo) for d in self._dimensions.values()])

        dim_val.strip()
        if not dim_val in self._dimension_accufuncs:
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
        for d, acc_vals in self._dimension_accufuncs.items():
            if self._accu_selector(dict([(a.key(), a.val()) for a in acc_vals])):
                rows[d] = acc_vals
        return ret

    def get_dimension_rows(self):
        '''
            return list of dict{aggre func key -> AccuFuncCls}
        '''
        rows = []
        for d, acc_vals_row in self._dimension_accufuncs.items():
            if not self._accu_selector or \
                    self._accu_selector(dict([(a.key(), a.val()) for a in acc_vals_row.values()])):
                acc_vals_row[self._dim_name] = d
                rows.append(acc_vals_row)

        return rows


    def get_accu_func(self):
        return [f() for f in self._accu_func_creators]

    def get_dim_name(self):
        return self._dim_name

