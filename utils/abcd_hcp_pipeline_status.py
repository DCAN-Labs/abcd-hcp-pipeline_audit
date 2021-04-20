import os
import json
import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
from .bids import s3_client

def parse_s3_status_json(access_key,host,secret_key,bucketName,key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    posix_key = key.replace('%2F','/')
    s3_object = client.get_object(Bucket=bucketName,Key=posix_key)
    s3_object_body = s3_object['Body']
    node_status = json.loads(s3_object_body.read())['node_status']
    if node_status == 1:
        status='ok'
    elif node_status == 2:
        pass
    elif node_status == 3:
        status='failed'
    elif node_status == 4:
        status = 'NO_ABCD-HCP'
    elif node_status == 999:
        staus = 'not sure'
    return status


def s3_abcd_hcp_struct_outputs(bids_prefix,bucketName,access_key,derivatives_prefix,secret_key,host,subject,scanning_session,client):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)

    try:
        t1_suffix='T1w.nii.gz'
        bidst1 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=bids_prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='')
    except:
        bidst1=[]
        pass
    try:
        t2_suffix='T2w.nii.gz'
        bidst2 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=bids_prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='') 
    except: 
        try:
            t2_suffix='FLAIR.nii.gz'
            bidst2 = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            MaxKeys=1000,
                                            Prefix=bids_prefix,
                                            ContinuationToken='',
                                            FetchOwner=False,
                                            StartAfter='')
        except:
            bidst2=[]
            pass

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
    
def s3_abcd_hcp_struct_status(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='status.json'
    try:
        list_objects = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                        MaxKeys=1000,
                                        Prefix=prefix + '/logs/PostFreeSurfer',
                                        ContinuationToken='',
                                        FetchOwner=False,
                                        StartAfter='')
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status
    try:
        stage_status=''
        for obj in list_objects['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                stage_status = parse_s3_status_json(access_key=access_key,host=host,secret_key=secret_key,bucketName=bucketName,key=key)
        if not stage_status:
            stage_status = 'NO_ABCD-HCP'
        return stage_status
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status

def abcd_minimal_func_hcp_status_outputs(output_dir):
    pass


def s3_abcd_hcp_minimal_func_status(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='status.json'
    try:
        list_objects = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                        MaxKeys=1000,
                                        Prefix=prefix + '/logs/FMRISurface',
                                        ContinuationToken='',
                                        FetchOwner=False,
                                        StartAfter='')
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status
    try:
        stage_status=''
        for obj in list_objects['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                stage_status = parse_s3_status_json(access_key=access_key,host=host,secret_key=secret_key,bucketName=bucketName,key=key)
            if not stage_status:
                stage_status = 'NO_ABCD-HCP'
        return stage_status
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status
    
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

def s3_abcd_hcp_DCANBoldPreProc_func_outputs(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_DCANBOLDProc_v4.0.0_Atlas.dtseries.nii'
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

def s3_abcd_hcp_DCANBoldPreProc_func_status(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='status.json'
    try:
        list_objects = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                        MaxKeys=1000,
                                        Prefix=prefix + '/logs/DCANBOLDProcessing',
                                        ContinuationToken='',
                                        FetchOwner=False,
                                        StartAfter='')
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status
    try:
        stage_status=''
        for obj in list_objects['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                stage_status = parse_s3_status_json(access_key=access_key,host=host,secret_key=secret_key,bucketName=bucketName,key=key)
        if not stage_status:
            stage_status = 'NO_ABCD-HCP'
        return stage_status
    except KeyError:
        stage_status = 'NO_ABCD-HCP'
        return stage_status