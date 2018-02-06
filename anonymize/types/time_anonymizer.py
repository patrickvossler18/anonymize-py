# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import datetime

from .. import exceptions


class TimeAnonymizer:
    """
    Time columns anonymizer
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def anonymize(self, series, key=None, precision='s', timediff_max=100000):
        shift = None
        if not key:
            key = {}
            key['kind'] = 'time'
            key['precision'] = precision
        else:
            key = key.copy()
            if key.get('type', None) != 'time':
                if self.anonymizer.raise_exceptions:
                    raise exceptions.WrongKeyType(key.get('type', 'None'),
                                                  'time')
                else:
                    print("Wrong key type '%s' in time anonymizer",
                          (key.get('type', 'None')))
            shift = key.get('shift', None)
        min_val = np.min(series)
        max_val = np.max(series)
        if shift is None:
            abs_diff = (max_val - min_val).total_seconds()
            abs_diff = abs_diff if abs_diff else timediff_max
            shift = np.random.random_integers(0,
                                              np.min([abs_diff,
                                                      timediff_max]))
            if precision == 'D':
                shift_td = datetime.timedelta(shift)
            else:
                shift_td = datetime.timedelta(0, shift)
        key['shift'] = shift
        ret = series.apply(lambda x: (x-shift_td))
        return (ret, key)
