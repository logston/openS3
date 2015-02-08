from contextlib import closing
from datetime import datetime
import hashlib
import hmac
from http.client import HTTPConnection
import os
import urllib.parse
from wsgiref.handlers import format_date_time
from xml.etree import ElementTree

from .constants import (
    CONTENT_TYPES, ENCODING, VALID_MODES, DEFAULT_CONTENT_TYPE, OBJECT_URL_SCHEME,
    AWS_S3_REGION, AWS_S3_SERVICE)
from .utils import (
    validate_values, b64_string, S3FileDoesNotExistError, S3IOError,
    get_canonical_query_string, get_canonical_headers_string,
    get_signing_key, hmac_sha256, uri_encode, get_dirs_and_files)


class OpenS3(object):
    """
    A context manager for interfacing with S3.
    """
    def __init__(self, bucket, access_key, secret_key):
        """
        Create a new context manager for interfacing with S3.

        :param bucket: An S3 bucket
        :param access_key: An AWS access key (eg. AEIFKEKWEFJFWA)
        :param secret_key: An AWS secret key.
        """
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        validate_values(validation_func=lambda value: value is not None, dic=locals())
        self.netloc = '{}.s3.amazonaws.com'.format(bucket)
        self.mode = 'rb'
        self.acl = 'private'

        # File like attributes
        self.object_key = ''
        self.buffer = ''
        self._content_type = None
        self.response_headers = {}
        self.extra_request_headers = {}

    def __call__(self, *args, **kwargs):
        return self.open(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def read(self):
        """
        Return a bytes object with the contents of the remote S3 object.

        :rtype bytes:
        """
        self._get()
        return self.buffer

    def write(self, content):
        """
        Write content to file in S3.

        :param content:
        """
        if self.mode not in ('wb', 'ab'):
            raise RuntimeError('Must open file in write or append mode to write to file.')
        self.buffer = content
        # TODO handle multiple writes to same file.

    def open(self, object_key,
             mode='rb', content_type=None, acl='private', extra_request_headers=None):
        """
        Configure :py:class:`OpenS3` object to write to or read from a specific S3 object.

        :param object_key: A unique identifier for an object a bucket.
        :param mode: The mode in which the S3 object is opened. See Modes below.
        :param content_type: A standard MIME type describing the format of the contents.
        :param acl: Name of a specific canned Access Control List to apply to the object.

        **Modes**

        ====  ===================================================================
        mode  Description
        ====  ===================================================================
        'rb'  open for reading (default)
        'wb'  open for writing, truncating the file first
        'ab'  open for writing, appending to the end of the file if it exists
        ====  ===================================================================

        **Access Control List (acl)**

        Valid values include:

            - private  (*default*)
            - public-read
            - public-read-write
            - authenticated-read
            - bucket-owner-read
            - bucket-owner-full-control
        """
        if mode not in VALID_MODES:
            raise ValueError('{} is not a valid mode for opening an S3 object.'.format(mode))

        self.object_key = object_key
        self.mode = mode
        self.content_type = content_type
        self.acl = acl
        self.extra_request_headers = extra_request_headers if extra_request_headers else {}
        return self

    def close(self):
        if self.mode in ('wb', 'ab') and self.buffer:
            # TODO Does the old file need to be deleted
            # TODO from S3 before we write over it?
            self._put()
        # Reset OpenS3 object
        self.__init__(self.bucket, self.access_key, self.secret_key)

    @property
    def content_type(self):
        """
        Return content_type of file. If file does not
        have a content_type, make a guess.
        """
        if self._content_type:
            return self._content_type

        # Check the response headers
        if 'Content-Type' in self.response_headers:
            return self.response_headers['Content-Type']

        content_type = DEFAULT_CONTENT_TYPE
        # get file extension
        if self.object_key:
            _, extension = os.path.splitext(self.object_key)
            extension = extension.strip('.')
            if extension in CONTENT_TYPES:
                # Make an educated guess about what the Content-Type should be.
                content_type = CONTENT_TYPES[extension]

        return content_type

    @content_type.setter
    def content_type(self, content_type):
        self._content_type = content_type

    @property
    def size(self):
        """
        Return the size of the buffer, in bytes.
        """
        # The file hasn't been retrieved from AWS, retrieve it.
        if not self.buffer and not self.response_headers:
            self._get()
        return len(self.buffer)  # TODO is this the right way to get size of buffer (bytes)?

    @property
    def url(self):
        """Return URL of resource"""
        scheme = OBJECT_URL_SCHEME
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
            self.response_headers = response.headers
            return response

    def _get(self):
        """
        GET contents of remote S3 object.
        """
        request_headers = self._build_request_headers('GET', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('GET', self.object_key, headers=request_headers)
            response = conn.getresponse()
            if response.status not in (200, 204):
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
            self.response_headers = response.headers

    def _put(self):
        """PUT contents of file to remote S3 object."""
        request_headers = self._build_request_headers('PUT', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('PUT', self.object_key, self.buffer, headers=request_headers)
            response = conn.getresponse()
            if response.status not in (200, 204):
                raise S3IOError(
                    'openS3 PUT error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

    def delete(self):
        """
        Remove file from its S3 bucket.
        """
        headers = self._build_request_headers('DELETE', self.object_key)
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('DELETE', self.object_key, headers=headers)
            response = conn.getresponse()
            if response.status not in (200, 204):
                raise S3IOError(
                    'openS3 DELETE error. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))
        # Reset OpenS3 object
        self.__init__(self.bucket, self.access_key, self.secret_key)

    def exists(self):
        """
        Return ``True`` if file exists in S3 bucket.
        """
        response = self._head()
        if response.status in (200, 404):
            return response.status == 200
        raise S3IOError(
            'openS3 HEAD error. '
            'Response status: {}. '
            'Reason: {}. '
            'Response Text: \n'
            '{}'.format(response.status, response.reason, response.read()))

    def listdir(self):
        """
        Return a 2-tuple of directories and files in ``object_key``.

        :rtype: tuple
        """
        # Any mode besides 'rb' doesn't make sense when listing a
        # directory's contents.
        if self.mode != 'rb':
            raise ValueError('Mode must be "rb" when calling listdir.')
        # Ensure that self.object_key has the structure of a path,
        # rather than a file.
        if self.object_key[-1] != '/':
            raise ValueError('listdir can only operate on directories (ie. object keys that '
                             'end in "/"). Given key: {}'.format(self.object_key))

        if self.object_key[-1] != '/':
            raise ValueError('listdir can only operate on directories (ie. object keys that '
                             'end in "/"). Given key: {}'.format(self.object_key))

        if '/' in self.object_key.strip('/'):
            raise NotImplementedError('Listing subdirectories of bucket is not supported.')

        datetime_now = datetime.utcnow()
        iso_8601_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
        # Strip slashes from object_key. AWS doesn't like leading/trailing slashes
        # in the value associated with the prefix parameter.
        prefix = self.object_key.strip('/')
        query_string_dict = {'prefix': prefix} if prefix else {}
        header_dict = {
            'Host': self.netloc,
            'x-amz-date': iso_8601_timestamp,
            'x-amz-content-sha256': hashlib.sha256(''.encode()).hexdigest()
        }

        # Get Canonical Request
        http_verb = 'GET'
        canonical_uri = uri_encode('/')
        canonical_query_string = get_canonical_query_string(query_string_dict)
        canonical_headers = get_canonical_headers_string(header_dict) + '\n'
        signed_headers = ';'.join(header for header in sorted(header_dict.keys())).lower()
        hashed_payload = hashlib.sha256(''.encode()).hexdigest()
        canonical_request = '\n'.join((
            http_verb,
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            hashed_payload
        ))

        # Get StringToSign
        request_scope = ('{date}/{aws_region}/{aws_service}/aws4_request'
                         ''.format(date=datetime_now.strftime('%Y%m%d'),
                                   aws_region=AWS_S3_REGION,
                                   aws_service=AWS_S3_SERVICE))
        canonical_request_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        string_to_sign = '\n'.join((
            'AWS4-HMAC-SHA256',
            iso_8601_timestamp,
            request_scope,
            canonical_request_hash
        ))

        # Get SigningKey
        signing_key = get_signing_key(self.secret_key,
                                      datetime_now.strftime('%Y%m%d'),
                                      AWS_S3_REGION,
                                      AWS_S3_SERVICE)

        # Get Signature
        signature = hmac_sha256(signing_key, string_to_sign, digest=False).hexdigest()

        credential_str = '{access_key}/{scope}'.format(access_key=self.access_key,
                                                       scope=request_scope)
        authorization_str = ('AWS4-HMAC-SHA256 Credential={credential_str},'
                             'SignedHeaders={header_str},Signature={signature}'
                             ''.format(credential_str=credential_str,
                                       header_str=signed_headers,
                                       signature=signature))
        header_dict['Authorization'] = authorization_str

        # Build query string
        query_string = '?' + canonical_query_string if canonical_query_string else ''
        path = '/{}'.format(query_string)

        # Run query
        with closing(HTTPConnection(self.netloc)) as conn:
            conn.request('GET', path, headers=header_dict)
            response = conn.getresponse()
            response_body = response.read()
            if response.status not in (200, 204):
                raise S3IOError(
                    'openS3 GET error during listdir. '
                    'Response status: {}. '
                    'Reason: {}. '
                    'Response Text: \n'
                    '{}'.format(response.status, response.reason, response.read()))

        root = ElementTree.fromstring(response_body)
        # Work around xml namespace.
        aws_xmlns_namespace = root.tag.rstrip('ListBucketResult').strip('{}')
        namespaces = {'aws': aws_xmlns_namespace}
        key_element_list = root.findall('aws:Contents/aws:Key', namespaces)
        key_list = [k.text for k in key_element_list]
        # Strip leading slash from object_key since we need to match against
        # the keys without leading slashes that AWS returns.
        return get_dirs_and_files(key_list, self.object_key)

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

    def _build_request_headers(self, method, object_key):
        headers = dict()

        headers['Date'] = format_date_time(datetime.now().timestamp())
        # Only get size of file if there's data in the buffer.
        # Else will have some run away recursion.
        if self.buffer:
            headers['Content-Length'] = self.size
        headers['Content-MD5'] = self.md5hash
        headers['Content-Type'] = self.content_type
        headers['x-amz-acl'] = self.acl

        if self.extra_request_headers:
            headers.update(self.extra_request_headers)

        if self.access_key and self.secret_key:
            string_to_sign_list = [
                method,
                headers['Content-MD5'],
                headers['Content-Type'],
                headers['Date'],
                'x-amz-acl:{}'.format(headers['x-amz-acl']),
                '/' + self.bucket + object_key
            ]
            signature = self._request_signature('\n'.join(string_to_sign_list))
            headers['Authorization'] = ''.join(['AWS' + ' ', self.access_key, ':', signature])

        return headers
