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

    >>> from openS3 import OpenS3
    >>>
    >>> openS3 = OpenS3('my_bucket', '<access_key>', '<secret_key>')
    ... with openS3('/my/object/key.txt', mode='w') as fd:
    ...     fd.write('Yeah! Files going up to S3!')
    >>>
    >>> # Let's create a new OpenS3 object so we know we are not
    >>> # just printing saved state on attached to the OpenS3 object.
    >>> openS3 = OpenS3('my_bucket', '<access_key>', '<secret_key>')
    ... with openS3('/my/object/key.txt') as fd:
    ...     print(fd.read())
    b'Yeah! Files going up to S3!'
