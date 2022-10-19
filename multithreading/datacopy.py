import concurrent.futures
import os
import re
import boto3
from jproperties import Properties
from datetime import datetime


def get_year(file_name):
    print('Get Year for :' + file_name)
    year_regex = re.compile(r'{}'.format(configs.get("source.bucket.file.pattern").data))
    year = year_regex.search(file_name)
    print('Pattern found: ' + year.group())
    return year.group().strip("-_")[:4]


def download_and_upload(source_bucket_object):
    try:
        file_name = source_bucket_object.split('/')[-1]
        if file_name:
            print(file_name)
            year = get_year(file_name)
            print(year)
            sourceBucket.download_file(source_bucket_object, file_name)
            print('File downloaded temporarily')
            s3destination.meta.client.upload_file(file_name, destination_bucket_name,
                                                  destination_bucket_prefix + year + '/' + file_name)
            print('File uploaded to destination')
            os.remove(file_name)
            print('Removed temp file :' + file_name)
        else:
            print('Invalid File : ' + source_bucket_object)
    except Exception as ex:
        print(ex)
        print('Download failed for :' + source_bucket_object)


start_time = datetime.now()
configs = Properties()
with open("config/config.properties", "rb") as config_file:
    configs.load(config_file)

destination_session = boto3.session.Session(profile_name=configs.get("aws.account.destination.profile").data)
source_session = boto3.session.Session(profile_name=configs.get("aws.account.source.profile").data)

# Read configuration
s3destination = destination_session.resource('s3')
s3source = source_session.resource('s3')
source_bucket_name = configs.get("source.bucket.name").data
source_bucket_prefix = configs.get("source.bucket.prefix").data
destination_bucket_name = configs.get("destination.bucket.name").data
destination_bucket_prefix = configs.get("destination.bucket.prefix").data
sourceBucket = s3source.Bucket(source_bucket_name)
s3_source_client = source_session.client('s3')

process_pool_size = int(configs.get("process.pool.size").data)

try:

    paginator = s3_source_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket_name, Prefix=source_bucket_prefix)

    file_list = []

    for page in pages:
        for key in page['Contents']:
            file_list.append(key['Key'])

    print(len(file_list))

    executor = concurrent.futures.ProcessPoolExecutor(max_workers=process_pool_size)

    print('Process pool created for concurrent processing')

    if __name__ == '__main__':
        futures = [executor.submit(download_and_upload, sourceBucketObject) for sourceBucketObject in file_list]
        concurrent.futures.wait(futures)

    print('Processing completed')

    executor.shutdown()

    print('Shutting down process pool')

    end_time = datetime.now()
    time_diff = end_time - start_time
    print("Completed in seconds :", time_diff.seconds)
except Exception as e:
    print(e)
