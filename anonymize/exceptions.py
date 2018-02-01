# -*- coding: utf-8 -*-
# Module with all of the generic exceptions


class AnonymizerError(Exception):
    """ Base class for the exception """
    pass


class WrongDebugConfiguration(AnonymizerError):
    def __init__(self, action):
        self.message = 'One and only one variable between raise_exceptions' + \
                       'and quiet can be True'


class MissingColumns(AnonymizerError):
    def __init__(self, action):
        self.message = 'Missing data columns'


class MissingTypes(AnonymizerError):
    def __init__(self, action):
        self.message = 'Missing data types'


class MissingAction(AnonymizerError):
    def __init__(self, action):
        self.message = 'Unsupported action in transform_data: %s' % action


class UnsupportedType(AnonymizerError):
    def __init__(self, type):
        self.message = 'Unsupported type for change_type: %s' % type


class ReplacerMethodMissmatch(AnonymizerError):
    def __init__(self, expected, received):
        self.message = 'Missmatch in replacer. Expected: %s, but got: %s' % \
            (expected, received)


class RepeatedCollision(AnonymizerError):
    def __init__(self, retries):
        self.message = 'Persistent hash collision after %d retries' % retries


class WrongKeyType(AnonymizerError):
    def __init__(self, wrong_type, for_type):
        self.message = 'Wrong type %s for %s anonymizer' % \
            (wrong_type, for_type)


class WrongParameters(AnonymizerError):
    def __init__(self, message):
        self.message = message
