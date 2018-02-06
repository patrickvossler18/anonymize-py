# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import hashlib
import re
import datetime
import random

from . import exceptions
from .sequences.radix import AlphabetSequence


def anonymize(data,
              columns=None, key=None,
              pass_columns=[], skip_columns=[],
              low_cardinality_threshold=5,
              low_cardinality_alphabet=None,
              types={}, replacers={},
              heuristic_level=1,
              raise_exceptions=False, quiet=False):
    # Initialize returning object
    ret = pd.DataFrame()

    # Initialize keys map
    key = {} if not key else key
    key['name_map'] = key.get('name_map', {})
    key['name_map']['old_to_new'] = key['name_map'].get('old_to_new', {})
    key['name_map']['new_to_old'] = key['name_map'].get('new_to_old', {})
    key['data_map'] = key.get('data_map', {})

    # Determine types and do data transformation
    type_map, data, key = deduce_types(data, heuristic_level=heuristic_level,
                                       key=key,
                                       raise_exceptions=raise_exceptions,
                                       quiet=quiet)

    # Initialize columns
    if not columns:
        columns = data.columns
    elif type(columns) != 'list':
        columns = [columns]

    # Loop through columns
    idx = 0
    for column in columns:
        # Skip column if it's in skip_columns
        if column in skip_columns:
            continue

        # Don't change column if it's in pass_columns
        if column in pass_columns:
            ret[column] = data[column]
            key['data_map'][column] = {}
            key['name_map']['old_to_new'][column] = column
            key['name_map']['new_to_old'][column] = column

        # Get new name for the column
        new_column, key['name_map'] = column_name_replace(column, idx,
                                                          key['name_map'])

        # Get type and cardinality of the column and
        # perform corresponding replacement
        cardinality = len(data[column].unique())
        type = type_map[column]
        type = types.get(column, type)
        if replacers.get(type, False):
            replacers[type](data[column],
                            key=key['data_map'].get(new_column, {}))
        elif type == "datetime":
            ret[new_column], key['data_map'][new_column] = \
                date_replace(data[column],
                             key=key['data_map'].get(new_column, {}))
        elif cardinality <= low_cardinality_threshold or \
                cardinality <= np.log(len(data)):
            ret[new_column], key['data_map'][new_column] = \
                low_cardinality_replace(data[column],
                                        low_cardinality_alphabet,
                                        key=key['data_map'].get(new_column, {})
                                        )
        elif type == "int":
            ret[new_column], key['data_map'][new_column] = \
                int_replace(data[column],
                            key=key['data_map'].get(new_column, {}))
        elif type == "float":
            ret[new_column], key['data_map'][new_column] = \
                float_replace(data[column],
                              key=key['data_map'].get(new_column, {}))
        elif type == "email":
            ret[new_column], key['data_map'][new_column] = \
                email_replace(data[column],
                              key=key['data_map'].get(new_column, {}))
        else:
            ret[new_column], key['data_map'][new_column] = \
                generic_replace(data[column],
                                key=key['data_map'].get(new_column, {}),
                                alphabet=high_cardinality_alphabet,
                                raise_exceptions=raise_exceptions, quiet=quiet)
        idx += 1

    return (ret, key)


def column_name_replace(column, idx, key=None):
    key = {} if not key else key.copy()
    new_column = key.get('old_to_new', {}).get(column, None)
    new_column = 'col_'+str(idx) if new_column is None else new_column
    key['new_to_old'] = key.get('new_to_old', {})
    key['old_to_new'] = key.get('old_to_new', {})
    key['new_to_old'][new_column] = column
    key['old_to_new'][column] = new_column
    return (new_column, key)


def low_cardinality_replace(series, alphabet=None, key=None):
    elements = set(series.unique())
    key = {} if not key else key.copy()
    key['kind'] = 'low_cardinality'
    key['map'] = key.get('map', {})
    i = len(key['map'])
    if alphabet:
        alphabet_sequence = AlphabetSequence(alphabet, i)

    for element in sorted(elements - set(key['map'].keys())):
        if alphabet:
            key['map'][element] = alphabet_sequence.next()
        else:
            key['map'][element] = i
        i += 1

    ret = series.apply(lambda x: key['map'][x])
    return (ret, key)


def int_replace(series, key=None, threshold_rate=0.5, max_scale=1024):
    scale = None
    shift = None
    if not key:
        key = {}
        key['kind'] = 'int'
    else:
        key = key.copy()
        scale = key.get('scale', None)
        shift = key.get('shift', None)
    scale = np.random.random_integers(max_scale) if scale is None else scale
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


def float_replace(series, key=None):
    scale = None
    shift = None
    if not key:
        key = {}
        key['kind'] = 'float'
    else:
        key = key.copy()
        scale = key.get('scale', None)
        shift = key.get('shift', None)
    scale = np.random.random() * np.avg(series) if scale is None else scale
    ret = series.apply(lambda x: x*scale)
    min_val = np.min(ret)
    max_val = np.max(ret)
    if shift is None:
        shift = np.random.random(min_val, max_val) if min_val >= 0.0 else 0.0
    key['scale'] = scale
    key['shift'] = shift
    ret = series.apply(lambda x: (x-shift))+np.random.rand(len(series))*scale
    return (ret, key)


def date_replace(series, key=None, precision='s', timediff_max=100000):
    shift = None
    if not key:
        key = {}
        key['kind'] = 'date'
        key['precision'] = precision
    else:
        key = key.copy()
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
            shift_td = datetime.timedelta(int(shift))
        else:
            shift_td = datetime.timedelta(0, int(shift))
    key['shift'] = shift
    ret = series.apply(lambda x: (x-shift_td))
    return (ret, key)


def email_replace(series, key=None, collision_retries=10,
                  raise_exceptions=False, quiet=False):
    shift = None
    if not key:
        key = {}
        key['kind'] = 'email'
        key['domain_map'] = {}
        key['user_map'] = {}
    else:
        key = key.copy()
    emails = series.str.split("@")
    users = emails.str.get(0).unique()
    domains = emails.str.get(1).unique()

    def __replacer(value):
        user = value.str.get(0)
        domain = value.str.get(1)
        user_hash = hashlib.sha256(np.random.bytes(25) + user).hexdigest()
        domain_hash = hashlib.sha256(np.random.bytes(25) + domain).hexdigest()
        replacement = user_hash + "@" + domain_hash
        retry = 0
        while hash in key['map'] and retry < collision_retries:
            user_hash = hashlib.sha256(np.random.bytes(25) + user).hexdigest()
            domain_hash = hashlib.sha256(np.random.bytes(25) +
                                         domain).hexdigest()
            replacement = user_hash + "@" + domain_hash
            retry += 1
        if hash not in key['map']:
            key['map'][replacement] = keys[value.name]
        else:
            if raise_exceptions:
                raise exceptions.RepeatedCollision(retry)
            elif not quiet:
                print('Persistent collision after %d retries' % retry)
            return None
        return replacement

    ret = emails.apply(__replacer)
    return (ret, key)


def generic_replace(series, key=None, collision_retries=10,
                    raise_exceptions=False, quiet=False):
    if not key:
        key = {}
        key['kind'] = 'generic'
        key['map'] = {}

    def __replacer(value):
        hash = hashlib.sha256(np.random.bytes(25) + value).digest()
        retry = 0
        while hash in key['map'] and retry < collision_retries:
            hash = hashlib.sha256(np.random.bytes(25) + value).digest()
            retry += 1
        if hash not in key['map']:
            key['map'][hash] = key[value.name]
        else:
            if raise_exceptions:
                raise exceptions.RepeatedCollision(retry)
            elif not quiet:
                print('Persistent collision after %d retries' % retry)
            return None
        return hash

    ret = series.apply(__replacer)
    return (ret, key)


def deduce_type_from_dtype(dtype):
    dtype_map = {
        'bool': 'bool',
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
        type = 'generic'
    return type


def deduce_types(data, heuristic_level=1, key=None,
                 raise_exceptions=False, quiet=False):
    # Use dtype for numerical types: check if it's not a timestamp
    # For strings try date regexes and then wikidata lookups to distinguish
    # geography from other types
    key = key if key else {}
    if key.get('transformation_log', None):
        data, key = transform_data(data, key['transformation_log'], key,
                                   raise_exceptions=raise_exceptions,
                                   quiet=quiet)

    type_map = {}
    transformation_log = []
    location_fields = {}
    date_fields = {}
    date_field_idx = 0
    for column in data.columns:
        if column in type_map:
            continue
        series = data.loc[:, column]
        dtype = str(series.dtype)
        type_map[column] = deduce_type_from_dtype(dtype)
        # Do data transformation on heuristic level > 1
        if heuristic_level > 1:
            date_search = re.search(r'((.*[^A-Z]*)(Year|Month|Day|Date|Hour'
                                    '|Minute|Second)|(.*[^a-z]*)(year|month'
                                    '|day|date|hour|minute|second))(.*)',
                                    column)
            if date_search:
                date_groups = date_search.groups()
                field_prefix = date_groups[1] or date_groups[3]
                field_name = date_groups[2] or date_groups[4]
                field_suffix = date_groups[5]
                field_is_cap = field_name[0] != field_name[0].lower()
                field_date = field_prefix + \
                    ('Date' if field_is_cap else 'date') + \
                    field_suffix
                if field_name.lower() == 'date':
                    transformation_log.append(('change_type', column,
                                               'datetime'))
                else:
                    date_fields[field_date] = date_fields.get(field_date, {})
                    date_fields[field_date][field_name.lower()] = column
            elif type_map[column] == 'generic' and \
                    data.loc[:, column].str.strip().\
                    str.match(r'[0-9,.]+').all():
                transformation_log.append(('change_type', column, 'numeric'))

    for field in date_fields:
        if 'year' not in date_fields[field]:
            transformation_log.append(('add', field+'year', 1970, 'generic'))
            date_fields[field]['year'] = field+'year'
        if 'month' not in date_fields[field]:
            transformation_log.append(('add', field+'month', 1, 'generic'))
            date_fields[field]['month'] = field+'month'
        if 'day' not in date_fields[field]:
            transformation_log.append(('add', field+'day', 1, 'generic'))
            date_fields[field]['day'] = field+'day'
        if 'hour' not in date_fields[field]:
            transformation_log.append(('add', field+'hour', 0, 'generic'))
            date_fields[field]['hour'] = field+'hour'
        if 'minute' not in date_fields[field]:
            transformation_log.append(('add', field+'minute', 0, 'generic'))
            date_fields[field]['minute'] = field+'minute'
        if 'second' not in date_fields[field]:
            transformation_log.append(('add', field+'second', 0, 'generic'))
            date_fields[field]['second'] = field+'second'
        transformation_log.append(('combine', field, [
            date_fields[field]['year'],
            date_fields[field]['month'],
            date_fields[field]['day'],
            date_fields[field]['hour'],
            date_fields[field]['minute'],
            date_fields[field]['second']], '/'))
        transformation_log.append(('change_type', field,
                                   'datetime', '%Y/%m/%d/%H/%M/%S'))
        transformation_log.append(('remove', date_fields[field]['year']))
        transformation_log.append(('remove', date_fields[field]['month']))
        transformation_log.append(('remove', date_fields[field]['day']))
        transformation_log.append(('remove', date_fields[field]['hour']))
        transformation_log.append(('remove', date_fields[field]['minute']))
        transformation_log.append(('remove', date_fields[field]['second']))

    if transformation_log:
        data, key = transform_data(data, transformation_log, key,
                                   type_map=type_map,
                                   raise_exceptions=raise_exceptions,
                                   quiet=quiet)
        key['transformation_log'] = \
            key.get('transformation_log', []) + transformation_log

    return (type_map, data, key)


def transform_data(data, transformation_log, key, type_map=None,
                   raise_exceptions=False, quiet=False):
    data = data.copy()
    key = key.copy()
    for transformation in transformation_log:
        if transformation[0] == 'change_type':
            if transformation[2] == 'datetime':
                if len(transformation) <= 3:
                    data[transformation[1]] = \
                        pd.to_datetime(
                            data[transformation[1]],
                            infer_datetime_format=True, errors='coerce')
                else:
                    data[transformation[1]] = \
                        pd.to_datetime(
                            data[transformation[1]],
                            format=transformation[3], errors='coerce')
                key[transformation[1]] = key.get(transformation[1], {})
                key[transformation[1]]['kind'] = 'datetime'
                if type_map:
                    type_map[transformation[1]] = 'datetime'
            elif transformation[2] == 'numeric':
                data[transformation[1]] = \
                    pd.to_numeric(data[transformation[1]].str.strip().
                                  str.replace(',', ''), errors='coerce')
                key[transformation[1]] = key.get(transformation[1], {})
                dtype = str(data[transformation[1]].dtype)
                type = deduce_type_from_dtype(dtype)
                key[transformation[1]]['kind'] = type
                if type_map:
                    type_map[transformation[1]] = type
            elif transformation[2] == 'timedelta':
                data[transformation[1]] = \
                    pd.to_timedelta(data[transformation[1]], errors='coerce')
                key[transformation[1]] = key.get(transformation[1], {})
                key[transformation[1]]['kind'] = 'timedelta'
                if type_map:
                    type_map[transformation[1]] = 'timedelta'
            else:
                # Report an error
                if raise_exceptions:
                    raise exceptions.UnsupportedType(transformation[2])
                elif not quiet:
                    print('Unsupported type for change_type: %s' %
                          transformation[2])
        elif transformation[0] == 'combine':
            data.loc[:, transformation[1]] = \
                data.loc[:, transformation[2][0]].apply(str)
            for i in range(1, len(transformation[2])):
                data.loc[:, transformation[1]] = \
                    data.loc[:, transformation[1]].str.cat(
                        data.loc[:, transformation[2][i]].apply(str),
                        sep=transformation[3])
            key[transformation[1]] = key.get(transformation[1], {})
            key[transformation[1]]['kind'] = 'generic'
            if type_map:
                type_map[transformation[1]] = 'generic'
        elif transformation[0] == 'remove':
            del data[transformation[1]]
            if transformation[1] in key:
                del key[transformation[1]]
                if type_map:
                    del type_map[transformation[1]]
        elif transformation[0] == 'add':
            data[transformation[1]] = transformation[2]
            key[transformation[1]] = {'kind': transformation[3]}
            if type_map:
                type_map[transformation[1]] = transformation[3]
        else:
            # Report an error
            if raise_exceptions:
                raise exceptions.MissingAction(transformation[0])
            elif not quiet:
                print('Unsupported action in transform_data: %s' %
                      transformation[0])

    return (data, key)
