import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing

def get_bids_subjects(access_key,bucketName,host,prefix,secret_key):
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host,
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key)
    get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            Prefix=prefix,
                                             MaxKeys=1000,
                                             ContinuationToken='',
                                             FetchOwner=False,
                                             StartAfter='')
    bids_subjects = [item['Prefix'].split('/')[0] for item in get_data['CommonPrefixes'] if 'sub' in item['Prefix'].split('/')[0]]
    return bids_subjects

def get_bids_sessions(access_key,bucketName,host,prefix,secret_key):
    pdb.set_trace()
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host,
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key)
    
    get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
    bids_sessions = [item['Prefix'].split('/')[1] for item in get_data['CommonPrefixes'] if 'ses' in item['Prefix'].split('/')[1]]
    return bids_sessions