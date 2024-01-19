import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
import pdb
import glob
import os

def s3_client(access_key,host,secret_key):
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host,
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key)
    return client

    
def s3_get_bids_subjects(access_key,bucketName,host,prefix,secret_key):
	if len(prefix) > 0:
		if prefix[0] == '/':
			prefix = prefix[1:]
	prefix += 'sub-'
	client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
	paginator = client.get_paginator('list_objects_v2')
	page_iterator = paginator.paginate(Bucket=bucketName,Delimiter='/',Prefix=prefix,EncodingType='url',ContinuationToken='',
                                             FetchOwner=False,
                                             StartAfter='')
	get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                            Prefix=prefix,
                                             MaxKeys=1000,
                                             ContinuationToken='',
                                             FetchOwner=False,
                                             StartAfter='')
	bids_subjects = []
	for page in page_iterator:
		page_bids_subjects = ['sub-'+item['Prefix'].split('sub-')[1].strip('/') for item in page['CommonPrefixes'] if 'sub' in item['Prefix']]
		bids_subjects.extend(page_bids_subjects)
	return bids_subjects

def s3_get_bids_sessions(access_key,bucketName,host,prefix,secret_key):
    if len(prefix) > 0:
        if prefix[0] == '/':
            prefix = prefix[1:]
    prefix += 'ses-'
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    get_data = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
    bids_sessions = [item['Prefix'].split('/')[2] for item in get_data['CommonPrefixes'] if 'ses' in item['Prefix'].split('/')[2]]
    return bids_sessions

def s3_get_bids_structs(access_key,bucketName,host,prefix,secret_key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_T1w.nii.gz' # looking for at least a T1w file
    if prefix[0] == '/':
        prefix = prefix[1:]
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

            if key.endswith(suffix):
                return key
        return
    except KeyError:
        return


def s3_get_bids_funcs(access_key,bucketName,host,prefix,secret_key):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix='_bold.nii.gz' # looking for functional nifti files
    if prefix[0] == '/':
        prefix = prefix[1:]
    try:
        get_data = client.list_objects_v2(Bucket=bucketName,EncodingType='url',
                                          Prefix=prefix,
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
        #print(get_data['Contents'])    
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

def get_bids_structs(bids_dir):
    suffix='_T1w.nii.gz' # looking for at least a T1w file
    get_data = glob.glob(bids_dir+"/**/*"+suffix,recursive=True)
    anats = []
    for anat in get_data:
        if 'anat' in anat:
            anats.append(anat)
    return anats

def get_bids_funcs(bids_dir):
    suffix='_bold.nii.gz' # looking for functional nifti files
    get_data = glob.glob(bids_dir+"/**/*"+suffix,recursive=True)
    funcs = []
    for bold in get_data:
        if 'func' in bold: 

            # figure out functional basename
            try:
                task = bold.split('task-')[1].split('_')[0]
            except:
                raise Exception('this is not a BIDS folder. Exiting.')
            try:
                run = bold.split('run-')[1].split('_')[0]
            except:
                run=''
            try:
                acq = bold.split('acq-')[1].split('_')[0]
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


        
