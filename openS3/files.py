"""
    A custom ContentFile for S3 uploads/downloads.
"""
import hashlib
from io import BytesIO, UnsupportedOperation
import logging
import os

from py3s3.utils import b64_string
from py3s3.utils import ENCODING

logger = logging.getLogger(__name__)


class FileProxyMixin(object):
    """
    A mixin class used to forward file methods to an underlaying file
    object.  The internal file object has to be called "file"::

        class FileProxy(FileProxyMixin):
            def __init__(self, file):
                self.file = file
    """

    encoding = property(lambda self: self.file.encoding)
    fileno = property(lambda self: self.file.fileno)
    flush = property(lambda self: self.file.flush)
    isatty = property(lambda self: self.file.isatty)
    newlines = property(lambda self: self.file.newlines)
    read = property(lambda self: self.file.read)
    readinto = property(lambda self: self.file.readinto)
    readline = property(lambda self: self.file.readline)
    readlines = property(lambda self: self.file.readlines)
    seek = property(lambda self: self.file.seek)
    softspace = property(lambda self: self.file.softspace)
    tell = property(lambda self: self.file.tell)
    truncate = property(lambda self: self.file.truncate)
    write = property(lambda self: self.file.write)
    writelines = property(lambda self: self.file.writelines)
    xreadlines = property(lambda self: self.file.xreadlines)

    def __iter__(self):
        return iter(self.file)


class File(FileProxyMixin):
    DEFAULT_CHUNK_SIZE = 64 * 2**10

    def __init__(self, file, name=None):
        self._size = None
        self.file = file
        if name is None:
            name = getattr(file, 'name', None)
        self.name = name
        if hasattr(file, 'mode'):
            self.mode = file.mode

    def __str__(self):
        return smart_text(self.name or '')

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self or 'None')

    def __bool__(self):
        return bool(self.name)

    def __len__(self):
        return self.size

    def _get_size(self):
        if not hasattr(self, '_size') or self._size is None:
            if hasattr(self.file, 'size'):
                self._size = self.file.size
            elif hasattr(self.file, 'name') and os.path.exists(self.file.name):
                self._size = os.path.getsize(self.file.name)
            elif hasattr(self.file, 'tell') and hasattr(self.file, 'seek'):
                pos = self.file.tell()
                self.file.seek(0, os.SEEK_END)
                self._size = self.file.tell()
                self.file.seek(pos)
            else:
                raise AttributeError("Unable to determine the file's size.")
        return self._size

    def _set_size(self, size):
        self._size = size

    size = property(_get_size, _set_size)

    def _get_closed(self):
        return not self.file or self.file.closed
    closed = property(_get_closed)

    def chunks(self, chunk_size=None):
        """
        Read the file and yield chucks of ``chunk_size`` bytes (defaults to
        ``UploadedFile.DEFAULT_CHUNK_SIZE``).
        """
        if not chunk_size:
            chunk_size = self.DEFAULT_CHUNK_SIZE

        try:
            self.seek(0)
        except (AttributeError, UnsupportedOperation):
            pass

        while True:
            data = self.read(chunk_size)
            if not data:
                break
            yield data

    def multiple_chunks(self, chunk_size=None):
        """
        Returns ``True`` if you can expect multiple chunks.

        NB: If a particular file representation is in memory, subclasses should
        always return ``False`` -- there's no good reason to read from memory in
        chunks.
        """
        if not chunk_size:
            chunk_size = self.DEFAULT_CHUNK_SIZE
        return self.size > chunk_size

    def __iter__(self):
        # Iterate over this file-like object by newlines
        buffer_ = None
        for chunk in self.chunks():
            chunk_buffer = BytesIO(chunk)

            for line in chunk_buffer:
                if buffer_:
                    line = buffer_ + line
                    buffer_ = None

                # If this is the end of a line, yield
                # otherwise, wait for the next round
                if line[-1] in ('\n', '\r'):
                    yield line
                else:
                    buffer_ = line

        if buffer_ is not None:
            yield buffer_

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def open(self, mode=None):
        if not self.closed:
            self.seek(0)
        elif self.name and os.path.exists(self.name):
            self.file = open(self.name, mode or self.mode)
        else:
            raise ValueError('The file cannot be reopened.')

    def close(self):
        self.file.close()


class ContentFile(File):
    """
    A File-like object that takes just raw content, rather than an actual file.
    """
    def __init__(self, content, name=None):
        super().__init__(content, name=name)
        self.size = len(content)

    def __str__(self):
        return 'Raw content'

    def __bool__(self):
        return True

    def open(self, mode=None):
        self.seek(0)

    def close(self):
        pass


class S3ContentFile(ContentFile):
    """
    Represents a single file object in S3. Acts more like a data container
    at the moment.
    """
    def __init__(self, content, name=None, mimetype=None):
        super().__init__(content, name)
        self._content = None
        self.content = content
        self.mimetype = mimetype
        self.pos = 0

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<{} ({} bytes)>'.format(self.name, self.size)

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, value):
        """
        Set content to byte string, encoding if necessary
        """
        if isinstance(value, bytes):
            self._content = value
        else:
            self._content = value.encode(ENCODING)
        self.size = len(value)

    def md5hash(self):
        """Return the MD5 hash string of the file content"""
        digest = hashlib.md5(self.content).digest()
        return b64_string(digest)

    def write(self, content):
        raise NotImplementedError

    def read(self, chunk_size=None):
        """
        Return chunk_size of bytes, starting from self.pos, from self.content.
        """
        if chunk_size:
            data = self.content[self.pos:self.pos + chunk_size]
            self.pos += len(data)
            return data
        else:
            return self.content

    def seek(self, pos):
        self.pos = pos

    def tell(self):
        return self.pos