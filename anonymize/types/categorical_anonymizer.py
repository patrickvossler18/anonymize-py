# -*- coding: utf-8 -*-
import pandas as pd

from .. import exceptions
from ..sequences.radix import AlphabetSequence
from .string_replacers import RandomHexReplacer


class CategoricalAnonymizer():
    """
    Categorical type anonymization.
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def __low_cardinality_anonymizer(self, series, key=None, alphabet=None):
        elements = set(series.unique())
        key = {} if not key else key.copy()
        key['type'] = 'categorical'
        key['subtype'] = 'low_cardinality'
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

    def __generic_anonymizer(
                self, series, key=None,
                replacer_class=RandomHexReplacer):
        if not key:
            key = {}
            key['type'] = 'categorical'
            key['subtype'] = 'generic'
            key['map'] = {}

        replacer_obj = replacer_class(self.anonymizer, key)
        ret = series.apply(replacer_obj.replacer)
        return (ret, key)

    def anonymize(self, series, key=None,
                  low_cardinality_threshold=None, alphabet=None):
        ret = None
        key = None
        low_cardinality_threshold = low_cardinality_threshold \
            if low_cardinality_threshold \
            else self.anonymizer.low_cardinality_threshold

        if len(series) <= low_cardinality_threshold:
            ret, key = self.__low_cardinality_anonymizer(series, key, alphabet)
        else:
            ret, key = self.__generic_anonymizer(series, key)

        return (ret, key)
