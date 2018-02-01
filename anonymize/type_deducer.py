# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import hashlib
import re
import datetime


class TypeDeducer():
    """
    Helper class used to deduce types of data columns.
    """
    skip_columns = []
    heuristic_level = 1
    deducers_chain = []

    def __init__(self, skip_columns=[],
                 deducers_chain=[],
                 heuristic_level=1):
        self.deducers_chain = [DTypeDeducer(self)] \
            if not deducers_chain else deducers_chain
        self.skip_columns = skip_columns

    def deduce(self, data):
        ret = pd.DataFrame()
        types = {}
        for deducer in deducers_chain:
            if issubclass(deducer, BlockTypeDeducer):
                new_ret, new_types = deducer(data)
                for column in new_types:
                    if new_types.get(column, {}).get('confidence', 1.0) > \
                       types.get(column, {}).get('confidence', 0.0):
                        types[column] = type
                        ret.loc[column, ] = series
            elif issubclass(deducer, ColumnTypeDeducer):
                for column in data.columns:
                    if column in self.skip_columns:
                        continue
                    series, type = deducer(data.loc[column, ])
                    if type.get('confidence', 1.0) > \
                       types.get(column, {}).get('confidence', 0.0):
                        types[column] = type
                        ret.loc[column, ] = series
        return (ret, types)


class Deducer():
    """
    Abstract deducer class. Has only one method: deduce()
    """

    def deduce():
        return None


class ColumnTypeDeducer(Deducer):
    """
    Abstract class for column type deducers.
    """


class BlockTypeDeducer(Deducer):
    """
    Abstract class for block type deducers.
    """


class DTypeDeducer(ColumnTypeDeducer):
    """
    Uses pandas' dtypes to deduce types.
    """
    deducer = None

    def __init__(self, deducer):
        self.deducer = deducer

    def deduce(self, series):
        dtype = series.dtype
        dtype_map = {
            'bool': 'categorical',
            'datetime': 'datetime',
            'timedelta': 'timedelta',
            'half': 'float',
            'single': 'float',
            'float': 'float',
            'intc': 'int',
            'int': 'int',
            'byte': 'int',
            'short': 'int',
            'longlong': 'int'
        }
        if dtype in dtype_map:
            type = dtype_map[dtype]
        elif dtype[:-1] in dtype_map:
            type = dtype_map[dtype[:-1]]
        elif dtype[:-2] in dtype_map:
            type = dtype_map[dtype[:-2]]
        elif dtype[1:] in dtype_map:
            type = dtype_map[dtype[1:]]
        elif dtype[1:-1] in dtype_map:
            type = dtype_map[dtype[1:-1]]
        elif dtype[1:-2] in dtype_map:
            type = dtype_map[dtype[1:-2]]
        elif dtype[0:8] in dtype_map:
            type = dtype_map[dtype[0:8]]
        elif dtype[0:9] in dtype_map:
            type = dtype_map[dtype[0:9]]
        else:
            type = 'categorical'
        return type
