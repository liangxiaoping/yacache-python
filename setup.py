#!/usr/bin/env python

__version__ = '0.1.0'

from setuptools import setup, find_packages


setup(
    name='yacache',
    version=__version__,
    description='Yet another python client for key-value store',
    author='liangxiaoping',
    install_requires=[
        'redis',
        'oslo.serialization>=1.0.0'
    ],
    packages=find_packages(),
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ]
)
