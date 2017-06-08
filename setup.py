"""
Anonymize from Anonymizely
~~~~~~~~~~~~~~~~~~~~~~~~~~

Anonymize is a data anonymization library designed to be used with Pandas.

Anonumization is easy with Anonymize!
"""

from setuptools import setup

import anonymize

VERSION = anonymize.__version__

setup(
    name='anonymize',
    version=VERSION,
    url='https://github.com/anonymizely/anonymize-py',
    license='3-clause BSD',
    author='Vitalii Ostrovskyi',
    author_email='vitalii@ostrovskyi.org.ua',
    description='Data anonymization library '
                'for Pandas',
    long_description=__doc__,
    packages=['anonymize'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'pandas>=1.8',
        'numpy>=1.8',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Cython',
        'Topic :: Scientific/Engineering',
    ],
    tests_require=['pytest'],
    test_suite='tests'
)
