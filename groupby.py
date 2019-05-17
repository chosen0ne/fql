#!/usr/bin/env python
# coding=utf8
#
#
# @file:    groupby
# @author:  chosen0ne(louzhenlin86@126.com)
# @date:    2019-05-16 11:04:32


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

        # dimension(str) -> list of AccuFuncCls
        self._dimension_accufuncs = {}

        self._dim_name = '&'.join([n for n in self._dimensions.keys()])

    def __call__(self, finfo):
        dim_val = '&'.join([d(finfo) for d in self._dimensions.values()])

        dim_val.strip()
        if not dim_val in self._dimension_accufuncs:
            self._dimension_accufuncs[dim_val] = []
            for f in self._accu_func_creators:
                self._dimension_accufuncs[dim_val].append(f())

        for f in self._dimension_accufuncs[dim_val]:
            f(finfo)

    def get_dimension_vals(self):
        if self._accu_selector is None:
            return self._dimension_accufuncs
        else:
            ret = {}
            for d in self._dimension_accufuncs:
                acc_vals = self._dimension_accufuncs[d]
                if self._accu_selector(dict([(a.key(), a.val()) for a in acc_vals])):
                    ret[d] = acc_vals

            return ret

    def get_accu_func(self):
        return [f() for f in self._accu_func_creators]

    def get_accu_func_count(self):
        return len(self._accu_func_creators)

    def get_dim_name(self):
        return self._dim_name

    def is_groupby(self):
        return not (len(self._dimensions) == 1 and '*' in self._dimensions)

