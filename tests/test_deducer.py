# -*- coding: utf-8 -*-
from anonymize import type_deducer as td
import pytest
import pandas as pd
import numpy as np


def test_init():
    columns = []
    types = {}
    type_deducer = td.TypeDeducer()
