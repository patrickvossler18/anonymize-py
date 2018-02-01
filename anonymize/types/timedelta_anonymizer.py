# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from .. import exceptions


class TimedeltaAnonymizer:
    """
    Timedelta columns anonymizer
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def anonymize(self, series, key=None, precision='s'):
        ret = None
        scale = None
        shift = None
        if not key:
            key = {}
            key['type'] = 'timedelta'
        else:
            key = key.copy()
            if key.get('type', None) != 'timedelta':
                if self.anonymizer.raise_exceptions:
                    raise exceptions.WrongKeyType(key.get('type', 'None'),
                                                  'timedelta')
                else:
                    print("Wrong key type '%s' in timedelta anonymizer",
                          (key.get('type', 'None')))
            else:
                key['type']
            scale = key.get('scale', None)
            shift = key.get('shift', None)
        scale = np.random.random_integers(max_scale) \
            if scale is None else scale
        ret = series.apply(lambda x: x*scale)
        min_val = np.min(ret)
        max_val = np.max(ret)
        ret = ret + np.random.random_integers(0, scale, len(ret))
        if shift is None:
            shift = np.random.random(min_val, max_val) if min_val >= 0 else 0
        key['scale'] = scale
        key['shift'] = shift
        ret = series.apply(lambda x: (x-shift))

        return (ret, key)
