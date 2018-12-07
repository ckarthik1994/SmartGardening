from __future__ import print_function
import boto3
import json
import os
import sys
import uuid

import pymysql.cursors
import json
     
s3_client = boto3.client('s3')
awsAccessId_default='###'
awsAccessKey_default='###'
region_default = '###'

awsAccessId = awsAccessId_default
awsAccessKey = awsAccessKey_default
region = region_default

#Database Credentials
userName = '###'
passwd = '###'

phoneNumber = "###"

def readData(bucket, key):
    print ("Getting data from S3 buckets")
    s3Client = boto3.client('s3',
            aws_access_key_id = awsAccessId,
            aws_secret_access_key = awsAccessKey,
            region_name = region)
    responseObj = s3Client.get_object(
    Bucket= bucket,
    Key = key
    )
    
    jsonObj = json.loads(responseObj['Body'].read())
    print(jsonObj)

    datetime = jsonObj['datetime']
    temperature = jsonObj['temperature']
    humidity = jsonObj['humidity']

    #print(datetime, temperature, humidity)
    insertIntoDB('sensordb', datetime, temperature, humidity)
 
def insertIntoDB(dbName, datetime, temperature, humidity):
    print("Insert into DB")
    print(datetime, temperature, humidity)
    connection = pymysql.connect(host='###',
                             user=userName,
                             password=passwd,
                             db=dbName,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    print("Insert into DB2")
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO `temp_humidity_sensor_data` (`timestamp`, `temp`, `humidity`) VALUES (%s, %s, %s)"
            cursor.execute(sql, (datetime, temperature, humidity))
        print("Insert into DB3")
        connection.commit()
        print("Inserted Record")

        if humidity < 55:
            message = "Humidy less than: "+ str(humidity)
            #sendMessage(phoneNumber, message)

    finally:
        connection.close()

def sendMessage(phoneNumber, message):
    snsClient = boto3.client("sns",
    aws_access_key_id = awsAccessId,
    aws_secret_access_key = awsAccessKey,
    region_name = region)

    # Send your sms message.
    response = snsClient.publish(
        PhoneNumber = phoneNumber,
        Message = message
    )
    
    print("Message Sent")
    print(response)

def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        upload_path = '/tmp/resized-{}'.format(key)

        readData(bucket, key)
    
