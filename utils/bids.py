import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
import pdb
from bids import BIDSLayout

def s3_client(access_key,host,secret_key):
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host,
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key)
    return client

    
def s3_get_bids_subjects(access_key,bucketName,host,prefix,secret_key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            Prefix=prefix,
                                             MaxKeys=1000,
                                             ContinuationToken='',
                                             FetchOwner=False,
                                             StartAfter='')
    bids_subjects = [item['Prefix'].split('/')[0] for item in get_data['CommonPrefixes'] if 'sub' in item['Prefix'].split('/')[0]]
    return bids_subjects

def s3_get_bids_sessions(access_key,bucketName,host,prefix,secret_key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    
    get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
    bids_sessions = [item['Prefix'].split('/')[1] for item in get_data['CommonPrefixes'] if 'ses' in item['Prefix'].split('/')[1]]
    return bids_sessions

def s3_get_bids_funcs(access_key,bucketName,host,prefix,secret_key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_bold.nii.gz' # looking for functional nifti files
    try:
        get_data = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
        
    except KeyError:
        return
    try:
        funcs = []
        for obj in get_data['Contents']:
            key = obj['Key']

            if 'func' in key and key.endswith(suffix):

                # figure out functional basename
                try:
                    task = key.split('task-')[1].split('_')[0]
                except:
                    raise Exception('this is not a BIDS folder. Exiting.')
                try:
                    run = key.split('run-')[1].split('_')[0]
                except:
                    run=''
                try:
                    acq = key.split('acq-')[1].split('_')[0]
                except:
                    acq=''
                if not run:
                    if not acq:
                        funcs.append('task-'+task)
                    else:
                        funcs.append('task-'+task+'_acq-'+acq)
                else:
                    if not acq:
                        funcs.append('task-'+task+'_run-'+run)
                    else:
                        funcs.append('task-'+task+'_acq-'+acq+'_run-'+run)
                    
        funcs = list(set(funcs))
        return funcs
    except KeyError:
        return


        
