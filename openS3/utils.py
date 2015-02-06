from base64 import b64encode
from datetime import datetime
import re

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
