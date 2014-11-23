openS3
======

Python 3 &amp; AWS S3


Installation
============

::

   $ pip install openS3

Usage
=====

::

  >>> with openS3('/path/to/file') as fd:
          fd.write('HEY!')
