.. openS3 documentation master file, created by
   sphinx-quickstart on Tue Jan 28 21:27:11 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

openS3
======

A pythonic way to upload and download from AWS S3.

.. toctree::
   :maxdepth: 2


Installation
------------

To install the latest stable version of openS3::

    $ pip install openS3

To install the latest development version::

    $ git clone git@github.com:logston/openS3.git
    $ cd openS3
    $ python setup.py install


Usage
-----

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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
