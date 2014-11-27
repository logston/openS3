import os
import unittest
from datetime import datetime

from openS3 import OpenS3


bucket = os.environ['AWS_S3_BUCKET']
access_key = os.environ['AWS_S3_ACCESS_KEY']
secret_key = os.environ['AWS_S3_SECRET_KEY']


class OpenS3TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datetime = datetime.now()

    def setUp(self):
        self.content = ''.join(['file uploaded at about ', str(self.datetime)])
        self.object_key = '/testdir/test.txt'

    def test__000_get_available_name(self):
        # TODO
        pass

    def test__mimetype(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertEqual(fd.mimetype, 'text/plain')

    def test__101_head_file_does_not_exist(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertFalse(fd.exists())

    def test__102_put_object_in_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key, mode='w') as fd:
            fd.write(self.content)

    def test__301_head_file_does_exist(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertTrue(fd.exists())

    def test__501_get_object_from_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            content = fd.read().decode()
            self.assertEqual(content, self.content)
            self.assertEqual(fd.headers['Content-Type'], 'text/plain')

    def test__701_delete_object_from_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            fd.delete()
            self.assertFalse(fd.exists())


class OpenS3LargeFileTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datetime = datetime.now()

    def setUp(self):
        self.content = '\n'.join([
            'This test content file was uploaded at about {}'.format(self.datetime),
            '\n'.join(str(n) for n in range(200000))
        ])
        self.object_key = '/testdir/large_test.txt'

    def test__mimetype(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertEqual(fd.mimetype, 'text/plain')

    def test__101_head_file_does_not_exist(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertFalse(fd.exists())

    def test__102_put_object_in_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key, mode='w') as fd:
            fd.write(self.content)

    def test__301_head_file_does_exist(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            self.assertTrue(fd.exists())

    def test__501_get_object_from_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            content = fd.read().decode()
            self.assertEqual(content, self.content)
            self.assertEqual(fd.headers['Content-Type'], 'text/plain')

    def test__701_delete_object_from_bucket(self):
        openS3 = OpenS3(bucket, access_key, secret_key)
        with openS3(self.object_key) as fd:
            fd.delete()
            self.assertFalse(fd.exists())


if __name__ == '__main__':
    unittest.main()