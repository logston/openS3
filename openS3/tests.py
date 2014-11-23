import datetime
import os
import time
import unittest

from py3s3.storage import S3ContentFile
from py3s3.storage import S3IOError
from py3s3.storage import S3Storage


BUCKET = os.getenv('AWS_S3_BUCKET', None)
AWS_ACCESS_KEY = os.getenv('AWS_S3_ACCESS_KEY', None)
AWS_SECRET_KEY = os.getenv('AWS_S3_SECRET_KEY', None)


class Py3s3S3StorageTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datetime = datetime.datetime.now()
        cls.modify_time_dt = None

    def setUp(self):
        self.test_content = ''.join([
            'This test content file was uploaded at about ',
            str(self.datetime)
        ])
        self.test_file_name = '/testdir/test.txt'
        self.file = S3ContentFile(self.test_content, self.test_file_name, '')
        self.storage = S3Storage('', BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY)

    def test__000_get_available_name(self):
        # TODO
        pass

    def test__000_get_content_type(self):
        self.assertEqual(self.storage._get_content_type(self.file), 'text/plain')

    def test__101_HEAD_returns_test_file_existance(self):
        self.assertFalse(self.storage.exists(self.test_file_name))

    def test__102_PUT_saves_test_file_to_s3(self):
        name = self.storage._save(self.test_file_name, self.file)
        self.assertEqual(name, self.test_file_name)
        self.__class__.modify_time_dt = datetime.datetime.utcnow()

    def test__301_HEAD_returns_test_file_existance(self):
        self.assertTrue(self.storage.exists(self.test_file_name))

    def test__302_HEAD_returns_correct_file_size(self):
        size = self.storage.size(self.test_file_name)
        self.assertEqual(size, self.file.size)

    def test__303_HEAD_returns_correct_modified_time(self):
        time_ = self.storage.modified_time(self.test_file_name)
        self.assertAlmostEqual(
            time_, self.__class__.modify_time_dt,
            delta=datetime.timedelta(seconds=60)
        )

    def test__304_HEAD_returns_correct_media_type(self):
        headers = self.storage._get_response_headers(self.test_file_name)
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test__501_GET_pulls_test_file_down(self):
        file = self.storage._open(self.test_file_name)
        self.assertEqual(self.file.content, file.content)

    def test__701_DELETE_deletes_test_file_from_s3(self):
        self.storage.delete(self.test_file_name)
        self.assertFalse(self.storage.exists(self.test_file_name))


class Py3s3S3StorageWithNamePrefixTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datetime = datetime.datetime.now()
        cls.modify_time_dt = None

    def setUp(self):
        self.test_content = ''.join([
            'This test content file was uploaded at about ',
            str(self.datetime)
        ])
        self.test_file_name = '/testdir/test.txt'
        self.file = S3ContentFile(self.test_content, self.test_file_name, '')
        self.storage = S3Storage('static', BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY)

    def test__000_get_available_name(self):
        # TODO
        pass

    def test__000_get_content_type(self):
        self.assertEqual(self.storage._get_content_type(self.file), 'text/plain')

    def test__101_HEAD_returns_test_file_existance(self):
        self.assertFalse(self.storage.exists(self.test_file_name))

    def test__102_PUT_saves_test_file_to_s3(self):
        name = self.storage._save(self.test_file_name, self.file)
        self.assertEqual(name, self.test_file_name)
        self.__class__.modify_time_dt = datetime.datetime.utcnow()

    def test__301_HEAD_returns_test_file_existance(self):
        self.assertTrue(self.storage.exists(self.test_file_name))

    def test__302_HEAD_returns_correct_file_size(self):
        size = self.storage.size(self.test_file_name)
        self.assertEqual(size, self.file.size)

    def test__303_HEAD_returns_correct_modified_time(self):
        time_ = self.storage.modified_time(self.test_file_name)
        self.assertAlmostEqual(
            time_, self.__class__.modify_time_dt,
            delta=datetime.timedelta(seconds=60)
        )

    def test__304_HEAD_returns_correct_media_type(self):
        headers = self.storage._get_response_headers(self.test_file_name)
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test__501_GET_pulls_test_file_down(self):
        file = self.storage._open(self.test_file_name)
        self.assertEqual(self.file.content, file.content)

    def test__701_DELETE_deletes_test_file_from_s3(self):
        self.storage.delete(self.test_file_name)
        self.assertFalse(self.storage.exists(self.test_file_name))


class Py3s3S3StorageLargeFileSizeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datetime = datetime.datetime.now()
        cls.modify_time_dt = None

    def setUp(self):
        self.test_content = '\n'.join([
            'This test content file was uploaded at about {}'.format(self.datetime),
            '\n'.join(str(n) for n in range(200000))
        ])
        self.test_file_name = '/testdir/large_test.txt'
        self.file = S3ContentFile(self.test_content, self.test_file_name, '')
        self.storage = S3Storage('', BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY)

    def test__000_get_available_name(self):
        # TODO
        pass

    def test__000_get_content_type(self):
        self.assertEqual(self.storage._get_content_type(self.file), 'text/plain')

    def test__101_HEAD_returns_test_file_existance(self):
        self.assertFalse(self.storage.exists(self.test_file_name))

    def test__102_PUT_saves_test_file_to_s3(self):
        name = self.storage._save(self.test_file_name, self.file)
        self.assertEqual(name, self.test_file_name)
        self.__class__.modify_time_dt = datetime.datetime.utcnow()

    def test__301_HEAD_returns_test_file_existance(self):
        self.assertTrue(self.storage.exists(self.test_file_name))

    def test__302_HEAD_returns_correct_file_size(self):
        size = self.storage.size(self.test_file_name)
        self.assertEqual(size, self.file.size)

    def test__303_HEAD_returns_correct_modified_time(self):
        time_ = self.storage.modified_time(self.test_file_name)
        self.assertAlmostEqual(
            time_, self.__class__.modify_time_dt,
            delta=datetime.timedelta(seconds=60)
        )

    def test__304_HEAD_returns_correct_media_type(self):
        headers = self.storage._get_response_headers(self.test_file_name)
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test__501_GET_pulls_test_file_down(self):
        file = self.storage._open(self.test_file_name)
        self.assertEqual(self.file.content, file.content)

    def test__701_DELETE_deletes_test_file_from_s3(self):
        self.storage.delete(self.test_file_name)
        self.assertFalse(self.storage.exists(self.test_file_name))
