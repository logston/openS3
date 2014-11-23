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

    >>> import openS3
    >>> with openS3('/my/files/path.txt', 'w') as fd:
    ...    fd.write("My file's content")
    >>>
    >>> with openS3('/my/files/path.txt') as fd:
    ...    print(fd.read())
    My file's content


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
