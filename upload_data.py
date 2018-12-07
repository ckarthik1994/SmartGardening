import boto3
import os
import time

aws_access_id = 'AKIAIOVWH7T7IEH3PQUA'
aws_secret_key = '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'

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_id, aws_secret_access_key=aws_secret_key)

while True:
    for fname in os.listdir('data'):
        if fname.endswith('.processed'):
            continue
        filename = os.path.join('data',fname)
        bucket_name = 'sensor-data-rpi'
        s3.upload_file(filename, bucket_name, filename)
        print('Uploaded file %s to S3' % filename)
        os.rename(filename, filename+'.processed')
    time.sleep(5)