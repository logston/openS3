Changelog
=========

0.2.0
-----

- Added ability to list contents of a "directory" on AWS S3. Directories in the context of OpenS3
  are object keys that end in a slash. Eg. "/static/css/"

0.1.5
-----

- Refactored :py:class:`~openS3.ctx_manager.OpenS3` into its own module, :py:mod:`~openS3.ctx_manager`.

0.1.4
-----

- Changed default ACL value to be *private*.
- Added :py:meth:`~openS3.ctx_manager.OpenS3.open` method.
- Updated :py:meth:`~openS3.ctx_manager.OpenS3.open` to able to be called with a dictionary of extra request headers.
- Defined specific modes in which an S3 object can be opened.

0.1.3
-----

- Updated docs with `Travis CI <https://travis-ci.org/logston/openS3>`_ badge.