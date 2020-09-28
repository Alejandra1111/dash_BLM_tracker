import unittest
import boto3
import sys
sys.path.append('..')

from s3DataAccess import *

class TestS3Access(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        bucket_name = 'kotasstorage1'
        session = boto3.Session()
        s3_client = session.client("s3")
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)

        cls.s3access = S3DataAccess(bucket_name, s3_client, s3_resource)
        cls.prefix1 = 'app_data/data_cumulative/city_date/all_v1/retweet/'
        cls.file1 = 'app_data/data_cumulative/city_date/all_v1/retweet/2020_all_retweets.json'

    def test_filename_access(self):    
        self.assertIn(self.file1, self.s3access.get_files(self.prefix1), 
            'filenames should include: ' + self.file1 )

    def test_data_access(self):
        self.assertEqual(type(self.s3access.get_data(self.file1)), 
            type(b'abc'), 'data in bypes should be retrieved.')


if __name__=='__main__':
    unittest.main()
