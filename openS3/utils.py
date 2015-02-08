from base64 import b64encode
from datetime import datetime
import hashlib
import hmac
import re
from urllib import parse

from .constants import ENCODING, AWS_DATETIME_FORMAT


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
    Raise a ``ValueError`` if ``validation_func`` does not return ``True``.
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


def get_canonical_query_string(query_string_dict):
    query_pairs = sorted(query_string_dict.items())
    query_strings = [uri_encode(p) + '=' + uri_encode(v) for p, v in query_pairs]
    return '&'.join(query_strings)


def get_canonical_headers_string(header_dict):
    header_pairs = sorted(header_dict.items())
    header_strings = [h.lower() + ':' + v.strip() for h, v in header_pairs]
    return '\n'.join(header_strings)


def uri_encode(string):
    return parse.quote(string)


# Source for function:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def hmac_sha256(key, msg, digest=True):
    m = hmac.new(key, msg.encode("utf-8"), hashlib.sha256)
    if digest:
        return m.digest()
    return m


# Source for function:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def get_signing_key(secret_key, date_stamp, region_name, service_name):
    k_date = hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = hmac_sha256(k_date, region_name)
    k_service = hmac_sha256(k_region, service_name)
    k_signing = hmac_sha256(k_service, "aws4_request")
    return k_signing


def get_dirs_and_files(key_list, prefix):
    """
    Return a 2-tuple of sets. The first set in the 2-tuple
    contains directory names and the second set contains files names.

    Example with an object_key of /static/. The leading slash from /
    get_dirs_and_files('/static/', ['static/css/ads.css', 'static/js/main.js', 'static/robots.txt'])
    {'css', 'js'} {'robots.txt'}
    """
    dirs = set()
    files = set()
    prefix = prefix.lstrip('/')
    prefix_len = len(prefix)
    for key in key_list:
        if key.startswith(prefix):
            key = key[prefix_len:]
        if '/' in key:
            dirs.add(key.split('/')[0])
        else:
            files.add(key)
    return dirs, files


class S3IOError(IOError):
    """
    Generic exception class for S3 communication errors.
    """


class S3FileDoesNotExistError(S3IOError):
    def __init__(self, name=None, msg=None):
        total_msg = 'File does not exist: {}'.format(name)
        if msg:
            total_msg += ' {}'.format(msg)
        super().__init__(total_msg)
