from base64 import b64encode
from contextlib import closing
from datetime import datetime
import hashlib
import hmac
from http.client import HTTPConnection
import os
import re
import urllib.parse
from wsgiref.handlers import format_date_time

from .constants import MEDIA_TYPES, ENCODING, AWS_DATETIME_FORMAT


def b64_string(byte_string):
    """
    Return an base64 encoded byte string as an ENCODING decoded string
    """
    return b64encode(byte_string).decode(ENCODING)


def get_valid_filename(string_to_clean):
    """
    Returns the given string converted to a string that can be used for a clean
    filename. Specifically, leading and trailing spaces are removed; other
    spaces are converted to underscores; and anything that is not a unicode
    alphanumeric, dash, underscore, or dot, is removed.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    string_to_clean = string_to_clean.strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', string_to_clean)


def validate_values(validation_func, dic):
    """
    Validate each value in ``dic`` by passing it through ``func``.
    Raise a ``ValueError`` if ``func`` does not return ``True``.
    """
    for value_name, value in dic.items():
        if not validation_func(value):
            raise ValueError('{} can not be {}'.format(value_name, value))


def strpawstime(timestamp):
    """
    Return datetime from parsed AWS header timestamp string.
    AWS Datetime Format:  Wed, 28 Oct 2009 22:32:00 GMT
    """
    return datetime.strptime(timestamp, AWS_DATETIME_FORMAT)


class S3IOError(IOError):
    pass


class S3FileDoesNotExistError(S3IOError):
    def __init__(self, name=None, msg=None):
        total_msg = 'File does not exist: {}'.format(name)
        if msg:
            total_msg += ' {}'.format(msg)
        super().__init__(total_msg)


class OpenS3(object):
    def __init__(self, bucket, access_key, secret_key):
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        validate_values(validation_func=lambda value: value is not None, dic=locals())
        self.netloc = '{}.s3.amazonaws.com'.format(bucket)
        self.mode = None
        self.remote_file_exits = False

        # File like attributes
        self.object_key = ''
        self.buffer = ''
        self._mimetype = None
        self.headers = {}

    def __call__(self, object_key, mode='r', mimetype=None):
        self.mode = mode
        self.object_key = object_key
        self.mimetype = mimetype
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mode == 'w' and self.buffer:
            # TODO Does the old file need to be deleted
            # TODO from S3 before we write over it?
            self._put()

    def read(self):
        """
        Return a bytes object with the contents of the remote S3 object.
        :return:
        :rtype bytes:
        """
        self._get()
        return self.buffer

    def write(self, content):
        if self.mode != 'w':
            raise RuntimeError('Must open file in write mode to write to file.')
        self.buffer = content

    @property
    def mimetype(self):
        """
        Return mimetype of file. If file does not
        have a mimetype, make a guess.
        """
        if self._mimetype:
            return self._mimetype

        mimetype = 'binary/octet-stream'
        # get file extension
        if self.object_key:
            _, extension = os.path.splitext(self.object_key)
            extension = extension.strip('.')
            if extension in MEDIA_TYPES:
                # Make an educated guess about what the Content-Type should be.
                mimetype = MEDIA_TYPES[extension]

        return mimetype

    @mimetype.setter
    def mimetype(self, mimetype):
        self._mimetype = mimetype

    @property
    def size(self):
        """
        Return the size of the buffer, in bytes.
        :return:
        """
        return len(self.buffer)  # TODO is the right way to get size of buffer (bytes)

    @property
    def url(self):
        """Return URL of resource"""
        scheme = 'http'
        path = self.object_key
        query = ''
        fragment = ''
        url_tuple = (scheme, self.netloc, path, query, fragment)
        return urllib.parse.urlunsplit(url_tuple)

    @property
    def md5hash(self):
        """Return the MD5 hash string of the file content"""
        digest = hashlib.md5(self.buffer.encode(ENCODING)).digest()
        return b64_string(digest)

    def _head(self):
        request_headers = self._build_request_headers('HEAD', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('HEAD', self.url, headers=request_headers)
            response = conn.getresponse()
            return response

    def _get(self):
        """
        GET contents of remote S3 object.
        """
        request_headers = self._build_request_headers('GET', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('GET', self.object_key, headers=request_headers)
            response = conn.getresponse()
            if not response.status in (200,):
                if response.length is None:
                    # length == None seems to be returned from GET requests
                    # to non-existing files
                    raise S3FileDoesNotExistError(self.object_key)
                # catch all other cases
                raise S3IOError(
                    'openS3 GET error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

            self.buffer = response.read()
            self.headers = response.headers

    def _put(self):
        """PUT contents of file to remote S3 object."""

        post_params = {
            'file_size': self.size,
            'file_hash': self.md5hash,
            'content_type': self.mimetype,
        }

        request_headers = self._build_request_headers('PUT', self.object_key,
                                                      post_params=post_params)

        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('PUT', self.object_key, self.buffer, headers=request_headers)
            response = conn.getresponse()

            if response.status not in (200,):
                raise S3IOError(
                    'openS3 PUT error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

    def delete(self):
        headers = self._build_request_headers('DELETE', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('DELETE', self.object_key, headers=headers)
            response = conn.getresponse()
            if not response.status in (204,):
                raise S3IOError(
                    'openS3 DELETE error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

    def exists(self):
        response = self._head()
        if response.status in (200, 404):
            return response.status == 200
        else:
            raise S3IOError(
                'openS3 HEAD error. '
                'Response status: {}. '
                'Reason: {}. '
                'Response Text: \n'
                '{}'.format(response.status, response.reason, response.read()))

    def _request_signature(self, string_to_sign):
        """
        Construct a signature by making an RFC2104 HMAC-SHA1
        of the following and converting it to Base64 UTF-8 encoded string.
        """
        digest = hmac.new(
            self.secret_key.encode(ENCODING),
            string_to_sign.encode(ENCODING),
            hashlib.sha1
        ).digest()
        return b64_string(digest)

    def _build_request_headers(self, method, object_key, post_params=None):
        headers = dict()
        timestamp = format_date_time(datetime.now().timestamp())
        headers['Date'] = timestamp

        string_to_sign_list = [method]

        if post_params:
            headers['Content-Length'] = post_params['file_size']
            headers['Content-MD5'] = post_params['file_hash']
            headers['Content-Type'] = post_params['content_type']
            headers['x-amz-acl'] = 'public-read'

            string_to_sign_list += [
                headers['Content-MD5'],
                headers['Content-Type'],
                headers['Date'],
                'x-amz-acl:public-read'
            ]
        else:
            string_to_sign_list += [
                '',
                '',
                timestamp
            ]

        string_to_sign_list.append('/' + self.bucket + object_key)

        signature = self._request_signature('\n'.join(string_to_sign_list))

        if self.access_key and self.secret_key:
            headers['Authorization'] = ''.join(['AWS' + ' ', self.access_key, ':', signature])

        return headers
