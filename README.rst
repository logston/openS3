openS3
======

|docs| |tests|

A Pythonic way to upload and download from AWS S3.


Installation
============

::

   $ pip install openS3

To install the latest development version::

    $ git clone git@github.com:logston/openS3.git
    $ cd openS3
    $ python setup.py install


Usage
=====

::

    >>> from openS3 import OpenS3
    >>>
    >>> openS3 = OpenS3('my_bucket', '<access_key>', '<secret_key>')
    ... with openS3('/my/object/key.txt', mode='wb') as fd:
    ...     fd.write('Yeah! Files going up to S3!')
    >>>
    >>> # Let's create a new OpenS3 object so we know we are not
    >>> # just printing saved state attached to the previous OpenS3 object.
    >>> openS3 = OpenS3('my_bucket', '<access_key>', '<secret_key>')
    ... with openS3('/my/object/key.txt') as fd:
    ...     print(fd.read())
    b'Yeah! Files going up to S3!'

Bug Tracker
===========

Please report bugs!!
`Report bugs at openS3's GitHub repo <https://github.com/logston/openS3/issues>`_.

Further Documentation
=====================

Further documentation can be found on `Read the Docs`_.

.. _Read the Docs: http://opens3.readthedocs.org/en/latest/

.. |docs| image:: https://readthedocs.org/projects/opens3/badge/
    :alt: Documentation Status
    :scale: 100%
    :target: http://opens3.readthedocs.org/en/latest/

.. |tests| image:: https://travis-ci.org/logston/openS3.svg
    :target: https://travis-ci.org/logston/openS3