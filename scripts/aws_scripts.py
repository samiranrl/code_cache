import boto3
import botocore
from boto3.s3.transfer import S3Transfer



# Download a file from s3

def s3_download(bucket, file_name, local_path):
    baseFileName = file_name.split('/')[-1]
    outputFile = local_path+baseFileName
    transfer = S3Transfer(boto3.client(service_name='s3', region_name=config['AWS_REGION'], aws_access_key_id = config['AWS_ACCESS_KEY_ID'], 
                                      aws_secret_access_key = config['AWS_SECRET_KEY_ID']))
    try:
        transfer.download_file(bucket, file_name, outputFile)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 404:
            print("Object not found")
        else:
            raise

# Upload a file to s3

def s3_upload(bucket, file_name, aws_path):
    baseFileName = file_name.split('/')[-1]
    outputFile = aws_path+baseFileName
    transfer = S3Transfer(boto3.client(service_name='s3', region_name=config['AWS_REGION'], aws_access_key_id = config['AWS_ACCESS_KEY_ID'], 
                                       aws_secret_access_key = config['AWS_SECRET_KEY_ID']))
    transfer.upload_file(file_name, bucket, outputFile)

# AWS configuration

config = {
        "AWS_ACCESS_KEY_ID" : "some access key",
        "AWS_SECRET_KEY_ID" : "some secret key",
        "AWS_REGION" : "us-west-2"
}



# boto basic instance management


import boto3
 
ids = ['instance_id']
ec2 = boto3.resource('ec2', aws_access_key_id=config['AWS_ACCESS_KEY_ID'], aws_secret_access_key=config['AWS_SECRET_KEY_ID'], region_name='ap-south-1')


try:
    print(ec2.instances.filter(InstanceIds = ids).stop()) # stop ec2
except Exception as e:
    print(e)

try:
    print(ec2.instances.filter(InstanceIds = ids).start()) # start ec2
except Exception as e:
    print(e)
    
for instance in ec2.instances.filter(InstanceIds = ids):
    print (instance.state) # check current state

for instance in ec2.instances.filter(InstanceIds = ids): # ssh script
    print ()
    print('ssh -i "sam_tildehat.pem" ubuntu@ec2-' + instance.public_ip_address.replace(".", "-") +'.ap-south-1.compute.amazonaws.com')
