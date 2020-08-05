import json
import boto3
from io import BytesIO

bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")

data_path = 'app_data/'


def getData(filename):
    f = BytesIO()
    s3_client.download_fileobj(bucket_name, filename, f)
    return f.getvalue()


def lambda_handler(event, context):
	city = event['city']
	date = event['date']
	
	city_date_stats_path = data_path + "data_cumulative/city_date/" + city + "/stats"
	filename = city_date_stats_path + '/stats_'+ date + '.json'
	stats = getData(filename)
	
	return stats
