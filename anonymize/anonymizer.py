# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import hashlib
import re
import datetime

from . import exceptions
from .types.int_anonymizer import IntAnonymizer
from .types.float_anonymizer import FloatAnonymizer
from .types.categorical_anonymizer import CategoricalAnonymizer
from .types.datetime_anonymizer import DatetimeAnonymizer
from .types.time_anonymizer import TimeAnonymizer
from .types.timedelta_anonymizer import TimedeltaAnonymizer
from .types.url_anonymizer import UrlAnonymizer
from .types.email_anonymizer import EmailAnonymizer


class Anonymizer():
    """
    Data anonymization class. Configured by constructor.
    Main method: anonymize().
    """
    key = None
    types = None
    columns = []
    pass_columns = []
    skip_columns = []
    low_cardinality_threshold = 5
    name_anonymizer = None
    name_based_anonymizers = {}
    type_based_anonymizers = {}
    raise_exceptions = False

    # Block anonymizers
    __blocks = []

    def __init__(self, columns, types, key=None,
                 pass_columns=[], skip_columns=[],
                 low_cardinality_threshold=5,
                 name_anonymizer=None,
                 name_based_anonymizers={},
                 type_based_anonymizers={},
                 raise_exceptions=False):
        """
        Configure anonymizer object through constructor.
        """
        # Initialize the key
        self.key = {} if not key else key
        self.key['name_map'] = self.key.get('name_map', {})
        self.key['name_map']['old_to_new'] = \
            self.key['name_map'].get('old_to_new', {})
        self.key['name_map']['new_to_old'] = \
            self.key['name_map'].get('new_to_old', {})
        self.key['data_map'] = self.key.get('data_map', {})

        # Initialize other attributes
        self.column = columns
        self.pass_columns = pass_columns
        self.skip_columns = skip_columns
        self.low_cardinality_threshold = low_cardinality_threshold
        self.types = types
        self.name_based_anonymizers = name_based_anonymizers
        self.type_based_anonymizers = type_based_anonymizers
        self.raise_exceptions = raise_exceptions
        self.name_anonymizer = self.column_name_anonymizer

        if not self.columns:
            if self.raise_exceptions:
                raise exceptions.MissingColumns()
            else:
                print("columns argument can not be empty or None")
                return None

        if not self.types:
            if self.raise_exceptions:
                raise exceptions.MissingTypes()
            else:
                print("types argument can not be empty or None")
                return None

        # Initialize types
        for type in set(self.types.values()):
            if type in self.type_based_anonymizers:
                continue
            elif type == 'int':
                self.type_based_anonymizers[type] = IntAnonymizer(self)
            elif type == 'float':
                self.type_based_anonymizers[type] = FloatAnonymizer(self)
            elif type == 'categorical':
                self.type_based_anonymizers[type] = CategoricalAnonymizer(self)
            elif type == 'email':
                self.type_based_anonymizers[type] = EmailAnonymizer(self)
            elif type == 'url':
                self.type_based_anonymizers[type] = UrlAnonymizer(self)
            elif type(type) == 'tuple':
                type = type[0]
                details = type[1]
                if type == 'datetime':
                    self.type_based_anonymizers[type] = \
                        DatetimeAnonymizer(self,
                                           details.get('date_format', ''))
                elif type == 'time':
                    self.type_based_anonymizers[type] = \
                        TimeAnonymizer(self, details.get('time_format', ''))
                elif type == 'timedelta':
                    self.type_based_anonymizers[type] = \
                        TimedeltaAnonymizer(self,
                                            details.get('precision', 's'))
                elif type == 'block':
                    columns = details.get('columns', [])
                    block_anonymizer = None
                    if details.get('subtype', None) == 'int':
                        block_anonymizer = \
                            BlockIntAnonymizer(self, columns)
                    elif details.get('subtype', None) == 'float':
                        block_anonymizer = \
                            BlockFloatAnonymizer(self, columns)
                    elif details.get('subtype', None) == 'categorical':
                        block_anonymizer = \
                            BlockCategoricalAnonymizer(self, columns)
                    else:
                        if self.raise_exceptions:
                            raise exceptions.UnsupportedType()
                        else:
                            print("Unsupported type: %s, %s", (type,
                                                               str(details)))
                    if block_anonymizer:
                        self.__blocks.append(block_anonymizer)
                else:
                    if self.raise_exceptions:
                        raise exceptions.UnsupportedType()
                    else:
                        print("Unsupported type: %s, %s", (type,
                                                           str(details)))
            else:
                if self.raise_exceptions:
                    raise exceptions.UnsupportedType()
                else:
                    print("Unsupported type: %s", type)

    def column_name_anonymizer(column, idx, key=None):
        new_column = key.get(column, {}).get('name', None)
        new_column = 'col_'+str(idx) if new_column is None else new_column
        return new_column

    def anonymize(self, data):
        ret = pd.DataFrame()
        key = None

        columns = data.columns

        for block in self.__blocks:
            block_id = block.signature()
            if block.columns:
                ret[block.columns, ], key[block_id] = \
                    block.anonymize(data.loc[block.columns, ],
                                    self.key[block_id])
            else:
                ret, key[block_id] = block.anonymize(data, key[block_id])

        for column in data.columns:
            if column in self.skip_columns:
                continue
            elif column in self.pass_columns:
                ret.loc[column, ], self.key[column] = (data.loc[column, ],
                                                       {'name': column})
            else:
                new_column = (self.name_anonymizer)(column, self.key)
                if column in self.name_based_anonymizers:
                    ret[new_column, ], key[column] = \
                        self.name_based_anonymizers[column] \
                            .anonymize(data.loc[column, ], self.key[column])
                elif column in self.types:
                    ret[new_column, ], key[column] = \
                        self.type_based_anonymizer[self.types[column]] \
                            .anonymize(data.loc[column, ], self.key[column])
                key[column] = key.get(column, {})
                key[column]['name'] = new_column

        self.key = key
        return ret
