from setuptools import setup
from os import path
import os
import setuptools.command.build_py
import distutils.cmd
import distutils.log
import setuptools
import subprocess


HERE = path.abspath(path.dirname(__file__))
PROJECT = 'irods_fsspec'

with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

with open(path.join(HERE, PROJECT, 'VERSION'), encoding='utf-8') as f:
    VERSION = f.read().strip()

setup(
    name=PROJECT,
    version=VERSION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    packages=[PROJECT],
    python_requires='>=3.6, <4',
    install_requires=['fsspec>=0.8.5', 'python-irodsclient>=0.8.6'],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            f'{PROJECT}={PROJECT}:console_entrypoint',
        ],
    },
    project_urls={
        'Bug Reports': f'https://github.com/magao-x/{PROJECT}/issues',
    },
)

setuptools.setup(
        entry_points={
                'fsspec.specs': [
                        'irods=irods_fsspec.IRODSFileSystem',
                    ],
            }
)
