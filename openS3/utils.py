from base64 import b64encode
import os
import re

# get keys, bucket name, and encoding from env
ENCODING = 'utf-8'


def b64_string(bytestring):
    """
    Return an base64 encoded byte string as an ENCODING decoded string
    """
    return b64encode(bytestring).decode(ENCODING)


media_types = {
    "bmp": "image/bmp",
    "css": "text/css",
    "gif": "image/gif",
    "html": "text/html",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "mp3": "audio/mpeg",
    "pdf": "application/pdf",
    "png": "image/png",
    "rtf": "text/rtf",
    "txt": "text/plain",
    "tiff": "image/tiff",
    "zip": "application/zip"
}


def get_valid_filename(s):
    """
    Returns the given string converted to a string that can be used for a clean
    filename. Specifically, leading and trailing spaces are removed; other
    spaces are converted to underscores; and anything that is not a unicode
    alphanumeric, dash, underscore, or dot, is removed.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = s.strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s)


def validate_values(func, dic):
    """
    Validate each value in ``dic`` by passing it through ``func``.
    Raise a ``ValueError`` if ``func`` does not return ``True``.
    """
    for value_name, value in dic.items():
        if not func(value):
            raise ValueError('{} can not be {}'.format(value_name, value))