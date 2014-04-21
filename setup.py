#!/usr/bin/env python

import os, sys

try:
    from setuptools import setup
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

with open('raft/version.txt') as v:
    version = v.read().strip()

classifiers = '''
Development Status :: 1 - Planning
License :: Public Domain
Programming Language :: Python :: 2.6
Programming Language :: Python :: 2.7
'''

setup(name='py-raft',
      version=version,
      description='The RAFT Consensus Algorithm',
      author='Toby Burress',
      author_email='kurin@delete.org',
      classifiers = filter(None, classifiers.split('\n')),
      package_data={'': ['version.txt']},
      packages=['raft'],
      install_requires=('msgpack-python'))
