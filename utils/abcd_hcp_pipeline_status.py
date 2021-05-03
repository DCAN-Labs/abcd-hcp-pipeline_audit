import os
import json
import boto3
import botocore
import glob
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
from .bids import s3_client
import pdb

def parse_s3_status_json(access_key,host,secret_key,bucketName,key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    posix_key = key.replace('%2F','/')
    s3_object = client.get_object(Bucket=bucketName,Key=posix_key)
    s3_object_body = s3_object['Body']
    node_status = json.loads(s3_object_body.read())['node_status']
    if node_status == 1:
        status='ok'
    elif node_status == 2:
        status='in process'
    elif node_status == 3:
        status='failed'
    elif node_status == 4:
        status = 'NO_ABCD-HCP'
    elif node_status == 999:
        status = 'not sure'
    else:
        status = 'pending'
    return status

def parse_status_json(json_file):
    with open(json_file,'r') as f:
        json_data = json.load(f)
    node_status = json_data['node_status']
    if node_status == 1:
        status='ok'
    elif node_status == 2:
        status='in process'
    elif node_status == 3:
        status='failed'
    elif node_status == 4:
        status = 'NO_ABCD-HCP'
    elif node_status == 999:
        status = 'not sure'
    else:
        status = 'pending'
    return status

   
def s3_abcd_hcp_struct_status(output_dir):
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

def abcd_hcp_struct_status(output_dir):
    suffix='status.json'
    get_status_json = glob.glob(output_dir+"/**/logs/PostFreeSurfer/"+suffix,recursive=True)
    if get_status_json:
        stage_status = parse_status_json(get_status_json[0]) # assuming that we only find one status.json
    else:
        stage_status = 'NO_ABCD-HCP'
    return stage_status

def abcd_minimal_func_hcp_status_outputs(output_dir):
    suffix='status.json'
    get_status_json = glob.glob(output_dir+"/**/logs/FMRISurface/"+suffix,recursive=True)
    if get_status_json:
        stage_status = parse_status_json(get_status_json[0]) # assuming that we only find one status.json
    else:
        stage_status = 'NO_ABCD-HCP'
    return stage_status

def abcd_hcp_DCANBoldPreProc_func_status(output_dir):
    suffix='status.json'
    get_status_json = glob.glob(output_dir+"/**/logs/DCANBOLDProcessing/"+suffix,recursive=True)
    if get_status_json:
        stage_status = parse_status_json(get_status_json[0]) # assuming that we only find one status.json
    else:
        stage_status = 'NO_ABCD-HCP'
    return stage_status

def s3_abcd_hcp_struct_outputs(bids_prefix,bucketName,access_key,derivatives_prefix,secret_key,host,subject,scanning_session,client):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)

def s3_abcd_hcp_minimal_func_outputs(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_Atlas.dtseries.nii'

def s3_abcd_hcp_DCANBoldPreProc_func_outputs(bucketName,access_key,secret_key,host,prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_DCANBOLDProc_v4.0.0_Atlas.dtseries.nii'
    
