# -*- coding: utf-8 -*-
import pandas as pd

from .. import exceptions
from .string_replacers import HashSha256Replacer


class EmailAnonymizer:
    """
    Email columns anonymizer
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def anonymize(self, series, key=None,
                  replacer_class=HashSha256Replacer):
        shift = None
        if not key:
            key = {}
            key['kind'] = 'email'
            key['map'] = {}
        else:
            key = key.copy()
            if key.get('type', None) != 'email':
                if self.anonymizer.raise_exceptions:
                    raise exceptions.WrongKeyType(key.get('type', 'None'),
                                                  'email')
                else:
                    print("Wrong key type '%s' in email anonymizer",
                          (key.get('type', 'None')))
        emails = series.str.split("@", 1, True)
        replacer_obj = replacer_class(self.anonymizer, key)
        emails = emails.apply(replacer_obj.replacer)
        emails = emails[0] + '@' + emails[1]
        return (emails, key)
