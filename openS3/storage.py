"""
    A custom Storage interface for storing files to S3.
"""
from contextlib import closing
from datetime import datetime
import hashlib
import hmac
from http.client import HTTPConnection
import itertools
import os
import urllib.parse
from wsgiref.handlers import format_date_time

from py3s3.files import File
from py3s3.files import S3ContentFile
from py3s3.utils import b64_string
from py3s3.utils import ENCODING
from py3s3.utils import media_types
from py3s3.utils import get_valid_filename
from py3s3.utils import validate_values


class S3IOError(IOError):
    pass


class S3FileDoesNotExistError(S3IOError):
    def __init__(self, name=None, msg=None):
        total_msg = 'File does not exist: {}'.format(name)
        if msg:
            total_msg += ' {}'.format(msg)
        super().__init__(total_msg)


class Storage(object):
    """
    A base storage class, providing some default behaviors that all other
    storage systems can inherit or override, as necessary.
    """

    # The following methods represent a public interface to private methods.
    # These shouldn't be overridden by subclasses unless absolutely necessary.

    def open(self, name, mode='rb'):
        """
        Retrieves the specified file from storage.
        """
        return self._open(name, mode)

    def save(self, name, file):
        """
        Saves new content to the file specified by name. The content should be
        a proper File object or any python file-like object, ready to be read
        from the beginning.
        """
        # Get the proper name for the file, as it will actually be saved.
        if name is None:
            name = file.name

        if not hasattr(file, 'chunks'):
            file = File(file, name=name)

        name = self.get_available_name(name)
        name = self._save(name, file)

        # Store filenames with forward slashes, even on Windows
        return name.replace('\\', '/')

    # These methods are part of the public API, with default implementations.

    def get_valid_name(self, name):
        """
        Returns a filename, based on the provided filename, that's suitable for
        use in the target storage system.
        """
        return get_valid_filename(name)

    def get_available_name(self, name):
        """
        Returns a filename that's free on the target storage system, and
        available for new content to be written to.
        """
        dir_name, file_name = os.path.split(name)
        file_root, file_ext = os.path.splitext(file_name)
        # If the filename already exists, add an underscore and a number (before
        # the file extension, if one exists) to the filename until the generated
        # filename doesn't exist.
        count = itertools.count(1)
        while self.exists(name):
            # file_ext includes the dot.
            name = os.path.join(dir_name, "{}_{}{}".format(file_root, next(count), file_ext))

        return name

    def path(self, name):
        """
        Returns a local filesystem path where the file can be retrieved using
        Python's built-in open() function. Storage systems that can't be
        accessed using open() should *not* implement this method.
        """
        raise NotImplementedError("This backend doesn't support absolute paths.")

    # The following methods form the public API for storage systems, but with
    # no default implementations. Subclasses must implement *all* of these.

    def delete(self, name):
        """
        Deletes the specified file from the storage system.
        """
        raise NotImplementedError('subclasses of Storage must provide a delete() method')

    def exists(self, name):
        """
        Returns True if a file referened by the given name already exists in the
        storage system, or False if the name is available for a new file.
        """
        raise NotImplementedError('subclasses of Storage must provide a exists() method')

    def listdir(self, path):
        """
        Lists the contents of the specified path, returning a 2-tuple of lists;
        the first item being directories, the second item being files.
        """
        raise NotImplementedError('subclasses of Storage must provide a listdir() method')

    def size(self, name):
        """
        Returns the total size, in bytes, of the file specified by name.
        """
        raise NotImplementedError('subclasses of Storage must provide a size() method')

    def url(self, name):
        """
        Returns an absolute URL where the file's contents can be accessed
        directly by a Web browser.
        """
        raise NotImplementedError('subclasses of Storage must provide a url() method')

    def accessed_time(self, name):
        """
        Returns the last accessed time (as datetime object) of the file
        specified by name.
        """
        raise NotImplementedError('subclasses of Storage must provide an accessed_time() method')

    def created_time(self, name):
        """
        Returns the creation time (as datetime object) of the file
        specified by name.
        """
        raise NotImplementedError('subclasses of Storage must provide a created_time() method')

    def modified_time(self, name):
        """
        Returns the last modified time (as datetime object) of the file
        specified by name.
        """
        raise NotImplementedError('subclasses of Storage must provide a modified_time() method')


class S3Storage(Storage):
    """
    A custom storage implimentation for use with py3s3.

    An instance of this class can be used to move a py3s3 file
    up to or down from AWS.
    """
    def __init__(self, name_prefix, bucket, aws_access_key, aws_secret_key):
        self.name_prefix = name_prefix
        config_values = {'bucket': bucket,
                         'aws_access_key': aws_access_key,
                         'aws_secret_key': aws_secret_key}
        validate_values(func=lambda value: value is not None, dic=config_values)
        self.bucket = bucket
        self.access_key = aws_access_key
        self.secret_key = aws_secret_key
        self.netloc = '{}.s3.amazonaws.com'.format(bucket)

    @staticmethod
    def request_timestamp(timestamp=None):
        if timestamp is None:
            timestamp = datetime.now().timestamp()
        return format_date_time(timestamp)

    @staticmethod
    def datetime_from_aws_timestamp(timestamp):
        """
        Return datetime from parsed AWS header timestamp string.
        AWS Format:  Wed, 28 Oct 2009 22:32:00 GMT
        """
        fmt = '%a, %d %b %Y %X %Z'
        return datetime.strptime(timestamp, fmt)

    def _prepend_name_prefix(self, name):
        """Return file name (ie. path) with the prefix directory prepended"""
        if not self.name_prefix:
            return name
        base = self.name_prefix
        if base[0] != '/':
            base = '/' + base
        if name[0] != '/':
            name = '/' + name
        return base + name

    def request_signature(self, stringtosign):
        """
        Construct a signature by making an RFC2104 HMAC-SHA1
        of the following and converting it to Base64 UTF-8 encoded string.
        """
        digest = hmac.new(
            self.secret_key.encode(ENCODING),
            stringtosign.encode(ENCODING),
            hashlib.sha1
        ).digest()
        return b64_string(digest)

    def _head(self, name):
        headers = self._request_headers('HEAD', self._prepend_name_prefix(name))
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('HEAD', self.url(name), headers=headers)
            return conn.getresponse()

    def _request_headers(self, method, prefixed_name, post_params=None):
        headers = dict()
        timestamp = self.request_timestamp()
        headers['Date'] = timestamp

        stringtosign_list = [method]

        if post_params:
            headers['Content-Length'] = post_params['file_size']
            headers['Content-MD5'] = post_params['file_hash']
            headers['Content-Type'] = post_params['content_type']
            headers['x-amz-acl'] = 'public-read'

            stringtosign_list += [
                headers['Content-MD5'],
                headers['Content-Type'],
                headers['Date'],
                'x-amz-acl:public-read'
            ]
        else:
            stringtosign_list += [
                '',
                '',
                timestamp
            ]

        stringtosign_list.append('/' + self.bucket + prefixed_name)

        signature = self.request_signature('\n'.join(stringtosign_list))

        if self.access_key and self.secret_key:
            headers['Authorization'] = ''.join(['AWS' + ' ', self.access_key, ':', signature])

        return headers

    def _get_response_headers(self, name):
        return dict(self._head(name).getheaders())

    def _get_content_type(self, file):
        """
        Return content type of file. If file does not
        have a content type, make a guess.
        """
        if file.mimetype:
            return file.mimetype

        # get file extension
        _, extension = os.path.splitext(file.name)
        extension = extension.strip('.')

        # Make an educated guess about what the Content-Type should be.
        return media_types[extension] if extension in media_types else 'binary/octet-stream'

    def _put_file(self, file):
        """Send PUT request to S3 with file contents"""

        post_params = {
            'file_size': file.size,
            'file_hash': file.md5hash(),
            'content_type': self._get_content_type(file),

        }

        headers = self._request_headers('PUT', file.prefixed_name, post_params=post_params)

        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('PUT', file.prefixed_name, file.read(), headers=headers)
            response = conn.getresponse()

            if response.status not in (200,):
                raise S3IOError(
                    'py3s3 PUT error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

    def _get_file(self, prefixed_name):
        """
        Return a signature for use in GET requests
        """
        headers = self._request_headers('GET', prefixed_name)
        file = S3ContentFile('')
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('GET', prefixed_name, headers=headers)
            response = conn.getresponse()
            if not response.status in (200,):
                if response.length is None:
                    # length == None seems to be returned from GET requests
                    # to non-existing files
                    raise S3FileDoesNotExistError(prefixed_name)
                # catch all other cases
                raise S3IOError(
                    'py3s3 GET error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

            file = S3ContentFile(response.read())
        return file

    def _open(self, name, mode='rb'):
        if mode not in ('rb',):
            S3IOError("No modes besides 'rb' are available for opening file from S3.")
        prefixed_name = self._prepend_name_prefix(name)
        file = self._get_file(prefixed_name)
        file.name = name
        return file

    def _save(self, name, file):
        mimetype = file.mimetype if hasattr(file, 'mimetype') and file.mimetype else ''
        file.prefixed_name = self._prepend_name_prefix(name)

        # Convert file to a S3ContentFile so file
        # can be pushed up to S3.
        if type(file) is not S3ContentFile:
            file = S3ContentFile(file.read(), file.prefixed_name, mimetype)
            file.prefixed_name = self._prepend_name_prefix(name)

        self._put_file(file)
        return name

    def delete(self, name):
        prefixed_name = self._prepend_name_prefix(name)
        headers = self._request_headers('DELETE', prefixed_name)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('DELETE', prefixed_name, headers=headers)
            response = conn.getresponse()
            if not response.status in (204,):
                raise S3IOError(
                    'py3s3 DELETE error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

    def exists(self, name):
        response = self._head(name)
        if response.status in (200, 404):
            return response.status == 200
        else:
            raise S3IOError(
                'py3s3 HEAD error. '
                'Response status: {}. '
                'Reason: {}. '
                'Response Text: \n'
                '{}'.format(response.status, response.reason, response.read()))

    def listdir(self, path):
        raise NotImplementedError()

    def size(self, name):
        return int(self._get_response_headers(name).get('Content-Length', 0))

    def url(self, name):
        """Return URL of resource"""
        scheme = 'http'
        path = self._prepend_name_prefix(name)
        query = ''
        fragment = ''
        url_tuple = (scheme, self.netloc, path, query, fragment)
        return urllib.parse.urlunsplit(url_tuple)

    def modified_time(self, name):
        dt_header = self._get_response_headers(name).get('Last-Modified')
        if dt_header is None:
            raise S3IOError('No modified time available for file: {}'.format(name))
        return self.datetime_from_aws_timestamp(dt_header)
