# -*- coding: utf-8 -*-
import pandas as pd

from .. import exceptions
from .string_replacers import HashSha256Replacer


class UrlAnonymizer:
    """
    Anonymizer of URL columns
    """
    anonymizer = None

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    def anonymize(self, series, key=None,
                  anonymization_parts=['protocol',
                                       'username',
                                       'password',
                                       'domain',
                                       'path',
                                       'query_string'
                                       ],
                  replacer_class=HashSha256Replacer):
        """
        URL anonymization. Takes anonymization_parts array with at least
        one component - domain
        """
        parts = pd.DataFrame(series)
        if not key:
            key = {}
            key['kind'] = 'url'
            key['anonymization_parts'] = anonymization_parts
        else:
            key = key.copy()
            if key.get('type', None) != 'url':
                if self.anonymizer.raise_exceptions:
                    raise exceptions.WrongKeyType(key.get('type', 'None'),
                                                  'url')
                else:
                    print("Wrong key type '%s' in url anonymizer",
                          (key.get('type', 'None')))
            anonymization_parts = key.get('anonymization_parts',
                                          anonymization_parts)
        if 'domain' not in anonymization_parts \
                and 'doman_components' not in anonymization_parts:
            if self.anonymizer.raise_exceptions:
                raise exceptions.WrongParameters("Url anonymizer's " +
                                                 "anonymization_parts is " +
                                                 "missing 'domain' part")
            else:
                print("Url anonymizer's anonymization_parts" +
                      " is missing 'domain' part")
        parts.columns = ['other']

        # Extract protocol
        parts_idx = parts['other'].str.contains('://')
        parts_tmp = \
            parts.loc[parts_idx, 'other'].str.split('://', 1)
        parts.loc[parts_idx, 'other'] = parts_tmp.str.get(-1)
        parts.loc[parts_idx, 'protocol'] = parts_tmp.str.get(0)
        parts.loc[~parts_idx, 'protocol'] = ''

        # Extract hash
        parts_idx = parts['other'].str.contains('#')
        parts_tmp = \
            parts.loc[parts_idx, 'other'].str.split('#', 1)
        parts.loc[parts_idx, 'hash'] = parts_tmp.str.get(-1)
        parts.loc[parts_idx, 'other'] = parts_tmp.str.get(0)
        parts.loc[~parts_idx, 'hash'] = ''

        # Extract username
        parts_idx = parts['other'].str.contains('@')
        parts_tmp = \
            parts.loc[parts_idx, 'other'].str.split('@', 1)
        parts.loc[parts_idx, 'username'] = parts_tmp.str.get(0)
        parts.loc[parts_idx, 'other'] = parts_tmp.str.get(-1)
        parts.loc[~parts_idx, 'username'] = ''

        # Extract password
        parts_idx = parts['username'].str.contains(':')
        parts_tmp = \
            parts.loc[parts_idx, 'username'].str.split(':', 1)
        parts.loc[parts_idx, 'username'] = parts_tmp.str.get(0)
        parts.loc[parts_idx, 'password'] = parts_tmp.str.get(-1)
        parts.loc[~parts_idx, 'password'] = ''

        # Extract query string
        parts_idx = parts['other'].str.contains('\?')
        parts_tmp = \
            parts.loc[parts_idx, 'other'].str.split('?', 1)
        parts.loc[parts_idx, 'other'] = parts_tmp.str.get(0)
        parts.loc[parts_idx, 'query_string'] = parts_tmp.str.get(1)
        parts.loc[~parts_idx, 'query_string'] = ''

        # Extract path and domain
        parts_idx = parts['other'].str.contains('/')
        parts_tmp = \
            parts.loc[parts_idx, 'other'].str.split('/', 20)
        parts.loc[parts_idx, 'domain'] = parts_tmp.str.get(0)
        parts.loc[parts_idx, 'path_components'] = parts_tmp.str.slice(1)
        parts.loc[parts_idx, 'path'] = \
            '/' + parts.loc[parts_idx, 'path_components'].str.join('/')
        parts.loc[~parts_idx, 'domain'] = parts.loc[~parts_idx, 'other']
        parts.loc[~parts_idx, 'path_components'] = [['']]
        parts.loc[~parts_idx, 'path'] = '/'

        # Extract domain components
        parts.loc[:, 'domain_components'] = \
            parts.loc[:, 'domain'].str.split('.', 20)

        # Extract query string components
        parts.loc[:, 'query_string_components'] = \
            parts.loc[:, 'query_string'].apply(
                    lambda l: [k.split('=', 1) for k in l.split('&', 20)]
                    )
        parts['query_string_variables'] = \
            parts['query_string_components'].apply(lambda l: [k[0] for k in l])
        parts['query_string_values'] = \
            parts['query_string_components'].apply(
                lambda l: [k[1] if len(k) > 1 else '' for k in l])

        # Free up memory
        del parts['other']
        del parts['query_string_components']
        del parts_tmp
        del parts_idx

        # Re-combine parts which are not in the anonymization_parts
        if 'password' not in anonymization_parts:
            parts_idx = parts['password'] != ''
            parts.loc[parts_idx, 'username'] = \
                parts.loc[parts_idx, 'username'] + ':' + \
                parts.loc[parts_idx, 'password']
            del parts['password']
        if 'username' not in anonymization_parts:
            parts_idx = parts['username'] != ''
            parts.loc[parts_idx, 'domain_components'] = \
                (parts['username'] + '@' + parts['domain_components']
                    .str.get(0)).str.split() + \
                parts['domain_components'].str.slice(1)
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, 'username'] + '@' + \
                parts.loc[parts_idx, 'domain']
            del parts['username']
        if 'protocol' not in anonymization_parts:
            parts_idx = parts['protocol'] != ''
            parts.loc[parts_idx, 'domain_components'] = \
                (parts['protocol'] + '://' + parts['domain_components']
                    .str.get(0)).str.split() + \
                parts.loc[parts_idx, 'domain_components'].str.slice(1)
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, 'protocol'] + '://' + \
                parts.loc[parts_idx, 'domain']
            del parts['protocol']

        qs_append_part = 'path'
        hash_append_part = 'query_string'
        if 'path' not in anonymization_parts \
                and 'path_components' not in anonymization_parts:
            parts.loc[:, 'domain_components'] = \
                parts.loc[:, 'domain_components'].str.slice(0, -1) + \
                (parts['domain_components']
                    .str.get(-1) + parts['path']).str.split()
            parts.loc[:, 'domain'] = \
                parts.loc[:, 'domain'] + \
                parts.loc[:, 'path']
            del parts['path']
            del parts['path_components']
            qs_append_part = 'domain'
        elif 'path' in anonymization_parts:
            del parts['path_components']
        else:
            del parts['path']

        if 'query_string' not in anonymization_parts \
                and 'query_string_components' not in anonymization_parts:
            parts_idx = parts['query_string'] != ''
            parts.loc[parts_idx, qs_append_part + '_components'] = \
                parts.loc[parts_idx, qs_append_part + '_components'] \
                .str.slice(0, -1) + \
                (parts[qs_append_part + '_components']
                    .str.get(-1) + '?' + parts['query_string']
                 ).str.split()
            parts.loc[parts_idx, qs_append_part] = \
                parts.loc[parts_idx, qs_append_part] + '?' + \
                parts.loc[parts_idx, 'query_string']
            del parts['query_string']
            del parts['query_string_components']
            hash_append_part = qs_append_part
        elif 'query_string' in anonymization_parts:
            del parts['query_string_variables']
            del parts['query_string_values']
        else:
            del parts['query_string']

        if 'hash' not in anonymization_parts:
            parts_idx = parts['hash'] != ''
            parts.loc[parts_idx, hash_append_part + '_components'] = \
                parts.loc[parts_idx, hash_append_part + '_components'] \
                .str.slice(0, -1) + \
                (parts[hash_append_part + '_components']
                    .str.get(-1) + '#' + parts['hash']
                 ).str.split()
            parts.loc[parts_idx, hash_append_part] = \
                parts.loc[parts_idx, hash_append_part] + '#' + \
                parts.loc[parts_idx, 'hash']
            del parts['hash']

        if 'domain_components' in anonymization_parts:
            del parts['domain']
        else:
            del parts['domain_components']

        # Anonymize parts
        replacer_obj = replacer_class(self.anonymizer, key)
        parts = parts.apply(replacer_obj.replacer)

        # Re-combine anonymized parts
        if 'domain_components' in anonymization_parts:
            parts['domain'] = parts['domain_components'].str.join('.')
            del parts['domain_components']
        if 'password' in anonymization_parts:
            parts_idx = parts['password'] != ''
            parts.loc[parts_idx, 'username'] = \
                parts.loc[parts_idx, 'username'] + ':' + \
                parts.loc[parts_idx, 'password']
            del parts['password']
        if 'username' in anonymization_parts:
            parts_idx = parts['username'] != ''
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, 'username'] + '@' + \
                parts.loc[parts_idx, 'domain']
            del parts['username']
        if 'protocol' in anonymization_parts:
            parts_idx = parts['protocol'] != ''
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, 'protocol'] + '://' + \
                parts.loc[parts_idx, 'domain']
            del parts['protocol']

        if 'path_components' in anonymization_parts:
            parts['path'] = '/' + parts['path_components'].str.join('/')
            del parts['path_components']
        if 'path' in anonymization_parts or \
                'path_components' in anonymization_parts:
            parts.loc[:, 'domain'] = \
                parts.loc[:, 'domain'] + \
                parts.loc[:, 'path']
            del parts['path']

        if 'query_string_components' in anonymization_parts:
            parts_idx = parts['query_string_variables'] != ''
            parts.loc[parts_idx, 'query_string'] = \
                (parts['query_string_variables'] +
                 parts['query_string_values']).apply(
                    lambda l: '&'.join([l[i]+'='+l[len(l)//2+i]
                                        if l[len(l)//2+i] and l[i] else ''
                                        for i in range(0, len(l)//2)]))
            del parts['query_string_variables']
            del parts['query_string_values']

        if 'query_string' in anonymization_parts or \
                'query_string_components' in anonymization_parts:
            parts_idx = parts['query_string'] != ''
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, 'domain'] + '?' + \
                parts.loc[parts_idx, 'query_string']
            del parts['query_string']
            del parts['query_string_components']

        if 'hash' in anonymization_parts:
            parts_idx = parts['hash'] != ''
            parts.loc[parts_idx, 'domain'] = \
                parts.loc[parts_idx, hash_append_part] + '#' + \
                parts.loc[parts_idx, 'hash']
            del parts['hash']

        return (parts['domain'], key)
