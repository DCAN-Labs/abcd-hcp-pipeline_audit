import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing

import pdb

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

def s3_abcd_hcp_struct_outputs(bucketName,access_key,secret_key,host,subject,scanning_session,client):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)

    try:
        t1_suffix='T1w.nii.gz'
        bidst1 = layout.get(subject=subject,session=session,extensions='.nii.gz',type='T1w')[0].filename
        bidst1 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='')
    except:
        bidst1=[]
        pass
    try:
        t2_suffix='T2w.nii.gz'
        bidst2 = layout.get(subject=subject,session=session,extensions='nii.gz',type='T2w')[0].filename
        bidst2 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='') 
    except: 
        try:
            t2_suffix='FLAIR.nii.gz'
            bidst2 = layout.get(subject=subject,session=session,extensions='nii.gz',type='T2w')[0].filename
            bidst2 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='')
        except:
            bidst2=[]
            pass

    struct_output_suffix = "{subject}.164k_fs_LR.wb.spec".format(sub=subject)
    try:
        client.head_object(Bucket=bucketName, Key=struct_output_suffix)
        abcd_hcp_t1 = True
    except:
        abcd_hcp_t1 = False
        pass
    if not bidst1 or not bidst2:
        struc_status="NO_BIDS"
    elif bidst1 and bidst2 and not abcd_hcp_t1:
        struc_status="NO_HCP"
    elif bidst1 and bidst2 and abcd_hcp_t1:
        struc_status="ok"

def s3_abcd_hcp_minimal_func_outputs(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_Atlas.dtseries.nii'
    try:
        list_objects = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
        
        
    except KeyError:
        return
    try:
        for obj in list_objects['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                return key
        return
    except KeyError:
        return

