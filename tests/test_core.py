# -*- coding: utf-8 -*-
import anonymize as anz
import pytest
import pandas as pd
import numpy as np


def test_check_deduce_types():
    df = pd.DataFrame(dict(A=np.random.rand(3),
                           B=1,
                           C='foo',
                           D=pd.Timestamp('20010102'),
                           E=pd.Series([1.0]*3).astype('float32'),
                           F=False,
                           G=pd.Series([1]*3, dtype='int8'),
                           H=pd.Timestamp('20010103') -
                           pd.Timestamp('20010102')))
    type_map, data, key = anz.deduce_types(df)
    assert(type_map['A'] == 'float')
    assert(type_map['B'] == 'int')
    assert(type_map['C'] == 'generic')
    assert(type_map['D'] == 'datetime')
    assert(type_map['E'] == 'float')
    assert(type_map['F'] == 'bool')
    assert(type_map['G'] == 'int')
    assert(type_map['H'] == 'timedelta')


def test_more_deduce_types():
    df = pd.DataFrame(dict(
            date='2001/01/01',
            created_year='2011',
            created_month=[12]*3,
            created_day=1,
            new_date='2001-01-11 02:34:56',
            updatedYear=2011,
            number='12',
            float_number='22.22',
            more='string'
            ))
    type_map, data, key = anz.deduce_types(df, heuristic_level=1)
    assert(type_map['date'] == 'generic')
    assert(type_map['created_year'] == 'generic')
    assert(type_map['created_month'] == 'int')
    assert(type_map['created_day'] == 'int')
    assert(type_map['new_date'] == 'generic')
    assert(type_map['updatedYear'] == 'int')
    assert(type_map['float_number'] == 'generic')
    assert(type_map['number'] == 'generic')
    assert(type_map['more'] == 'generic')
    type_map, data, key = anz.deduce_types(df, heuristic_level=2)
    assert(type_map['created_date'] == 'datetime')
    assert(type_map['new_date'] == 'datetime')
    assert(type_map['updatedDate'] == 'datetime')
    assert(type_map['float_number'] == 'float')
    assert(type_map['number'] == 'int')
    assert(type_map['more'] == 'generic')


def test_anonymize():
    df = pd.DataFrame(dict(
            date='2001/01/01',
            created_year='2011',
            created_month=[11, 10, 12],
            created_day=[1, 2, 3],
            new_date='2001-01-11 02:34:56',
            updatedYear=2011,
            number='12',
            float_number='22.22',
            more='string'
            ))
    data, key = anz.anonymize(df, heuristic_level=2)
