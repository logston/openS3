#! /usr/bin/env python

import os
import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

import openS3


with open(os.path.join(os.path.dirname(__file__), "README.rst")) as file:
    README = file.read()

with open(os.path.join(os.path.dirname(__file__), 'LICENSE')) as file:
    LICENSE = file.read()


class Tox(TestCommand):
    """Command to make python setup.py test run."""

    def finalize_options(self):
        super().finalize_options()
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # Do this import here because tests_require isn't processed
        # early enough to do a module-level import.
        from tox._cmdline import main
        sys.exit(main(self.test_args))


CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: BSD License",
    "Natural Language :: English",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3 :: Only",
    "Topic :: Internet",
    "Topic :: Internet :: WWW/HTTP",
    "Topic :: Utilities",
]


setup(name='openS3',
      version=openS3.__version__,
      author=openS3.__author__,
      author_email=openS3.__email__,
      maintainer=openS3.__author__,
      maintainer_email=openS3.__email__,
      url='http://github.com/logston/openS3',
      description='A pythonic way to upload and download from AWS S3.',
      long_description=README,
      license=LICENSE,
      classifiers=CLASSIFIERS,
      packages=['openS3'],
      include_package_data=True,
      package_data={'': ['LICENSE', 'README.rst']},
      tests_require=['tox'],
      cmdclass={'test': Tox})
