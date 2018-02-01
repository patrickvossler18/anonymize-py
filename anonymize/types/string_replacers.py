# -*- coding: utf-8 -*-
import random
import hashlib

from .. import exceptions


class HashSha256Replacer():
    """
    SHA256-based replacer
    """

    anonymizer = None
    key = {}
    salt = ''

    def __init__(self, anonymizer=None, key={}):
        self.anonymizer = anonymizer
        self.key = key
        if self.key.get('method', 'hash_sha256') != \
                'hash_sha256':
            if self.anonymizer.raise_exceptions:
                raise exceptions.ReplacerMethodMissmatch(
                            'hash_sha256',
                            self.key.get('method'))
            else:
                print('Missmatch in replacer. Expected: %s, but got: %s'
                      % ('hash_sha256', self.key.get('method')))
            return None

        self.key['method'] = 'hash_sha256'
        self.salt = self.key['salt'] = self.key.get(
                                        'salt',
                                        '%x' % random.randrange(16**25))

    def replacer(self, entry):
        hash = hashlib.sha256(salt + entry).hexdigest()
        if hash not in self.key['map']:
            self.key['map'][hash] = entry
        else:
            if self.anonymizer.raise_exceptions:
                raise exceptions.RepeatedCollision(0)
            else:
                print('Persistent collision after %d retries' % 0)
            return None
        return hash


class CollisionlessHashSha256Replacer():
    """
    Collisionless SHA256-based replacer
    """

    anonymizer = None
    key = {}
    collision_retries = 10

    def __init__(self, anonymizer=None, key={}):
        self.anonymizer = anonymizer
        self.key = key.copy()
        if self.key.get('method', 'hash_sha256_collisionless') != \
                'hash_sha256_collisionless':
            if self.anonymizer.raise_exceptions:
                raise exceptions.ReplacerMethodMissmatch(
                            'hash_sha256_collisionless',
                            self.key.get('method'))
            else:
                print('Missmatch in replacer. Expected: %s, but got: %s'
                      % ('hash_sha256_collisionless', self.key.get('method')))
            return None

        self.key['method'] = 'hash_sha256_collisionless'
        self.collision_retries = self.key['collision_retries'] = \
            self.key.get('collision_retries', 10)

    def replacer(entry):
        retry = 0
        if entry in key['map_reverse']:
            return key['map_reverse'][entry]
        while retry < collision_retries:
            hash = hashlib.sha256('%x' % random.randrange(16**25) +
                                  entry).hexdigest()
            if hash not in key['map']:
                break
        if hash not in key['map']:
            key['map'][hash] = entry
            key['map_reverse'][entry] = hash
        else:
            if self.anonymizer.raise_exceptions:
                raise exceptions.RepeatedCollision(retry)
            else:
                print('Persistent collision after %d retries' % retry)
            return None
        return hash


class RandomHexReplacer():
    """
    Random hex string replacer
    """

    anonymizer = None
    key = {}
    collision_retries = 10
    hex_length = 25

    def __init__(self, anonymizer=None, key={}):
        self.anonymizer = anonymizer
        self.key = key.copy()
        if self.key.get('method', 'random_hex_collisionless') != \
                'random_hex_collisionless':
            if self.anonymizer.raise_exceptions:
                raise exceptions.ReplacerMethodMissmatch(
                            'random_hex_collisionless',
                            self.key.get('method'))
            else:
                print('Missmatch in replacer. Expected: %s, but got: %s'
                      % ('random_hex_collisionless', self.key.get('method')))
            return None

        self.key['method'] = 'random_hex_collisionless'
        self.collision_retries = self.key['collision_retries'] = \
            self.key.get('collision_retries', 10)
        self.hex_length = self.key['hex_length'] = \
            self.key.get('hex_length', 25)

    def replacer(entry):
        retry = 0
        if entry in key['map_reverse']:
            return key['map_reverse'][entry]
        while retry < collision_retries:
            hash = '%x' % random.randrange(16**self.hex_length)
            if hash not in key['map']:
                break
        if hash not in key['map']:
            key['map'][hash] = entry
            key['map_reverse'][entry] = hash
        else:
            if self.anonymizer.raise_exceptions:
                raise exceptions.RepeatedCollision(retry)
            else:
                print('Persistent collision after %d retries' % retry)
            return None
        return hash
