from io import BytesIO
import boto3

bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)


class S3DataAccess:
    def __init__(self, bucket_name, s3_client, s3_resource):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.s3_resource = s3_resource
        self.bucket = s3_resource.Bucket(bucket_name)

    def get_data(self, filename):
        f = BytesIO()
        self.s3_client.download_fileobj(self.bucket_name, filename, f)
        return f.getvalue()

    def get_files(self, prefix):
        files = [object.key for object in self.bucket.objects.filter(Prefix=prefix)]
        return sorted(files)

