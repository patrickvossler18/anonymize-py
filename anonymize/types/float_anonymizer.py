# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from .. import exceptions


class FloatAnonymizer():
    """
    Floating point type anonymization.
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def anonymize(self, series, key=None):
        ret = None
        scale = None
        shift = None
        if not key:
            key = {}
            key['type'] = 'float'
        else:
            key = key.copy()
            if key.get('type', None) != 'float':
                if self.anonymizer.raise_exceptions:
                    raise exceptions.WrongKeyType(key.get('type', 'None'),
                                                  'float')
                else:
                    print("Wrong key type '%s' in float anonymizer",
                          (key.get('type', 'None')))
            else:
                key['type'] = 'float'
            scale = key.get('scale', None)
            shift = key.get('shift', None)
        scale = np.random.random() * np.avg(series) if scale is None else scale
        ret = series.apply(lambda x: x*scale)
        min_val = np.min(ret)
        max_val = np.max(ret)
        if shift is None:
            shift = np.random.random(min_val, max_val) \
                if min_val >= 0.0 else 0.0
        key['scale'] = scale
        key['shift'] = shift
        ret = series.apply(lambda x:
                           (x-shift))+np.random.rand(len(series))*scale
        return (ret, key)
