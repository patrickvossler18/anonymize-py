# -*- coding: utf-8 -*-
from anonymize import anonymizer as anz
import pytest
import pandas as pd
import numpy as np


def test_init():
    columns = []
    types = {}
    anonymizer = anz.Anonymizer(columns, types)
    data = pd.DataFrame()
    anonymizer.anonymize(data)
