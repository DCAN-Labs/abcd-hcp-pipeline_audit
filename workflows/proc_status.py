#!/usr/bin/env python3

"""
Created on Wed Dec 16 17:35:33 2020

Compares BIDS and S3-hosted HCP output to identify missing data

@author: roedi004
"""

import os
import sys
import subprocess
import json
from glob import glob
import re
import numpy as np
import pandas as pd
import boto3
from bids import BIDSLayout

def check_struct_proc_outputs_wf(bucketName,access_key,secret_key,host,subject,scanning_session,client):
    try: 
        bidst1 = layout.get(subject=subject,session=session,extensions='.nii.gz',type='T1w')[0].filename 
    except:
        bidst1 = []
        pass
    try:
        bidst2 = layout.get(subject=subject,session=session,extensions='nii.gz',type='T2w')[0].filename 
    except: 
        bidst2 = []
        pass
    s3t1t2 = "sub-{sub}/ses-{ses}/T1w/ses-{ses}/mri/T1wMulT2w_hires.nii.gz".format(sub=subject,ses=session)
    try:
        client.head_object(Bucket=bucketName, Key=s3t1t2)
        s3t1 = True
    except:
        s3t1 = False
        pass
    if not bidst1 or not bidst2:
        struc_status="NO_BIDS"
    elif bidst1 and bidst2 and not s3t1:
        struc_status="NO_HCP"
    elif bidst1 and bidst2 and s3t1:
        struc_status="ok"

def check_func_proc_outputs_wf(bucketName,access_key,secret_key,host,subject,scanning_session,bold):

    suffix='MSMAll_2_d40_WRN_hp2000_clean.dtseries.nii'
    try:
        list_objects = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix='{subject}/{session}/MNINonLinear/Results/{bold}/'.format(subject=subject,session=scanning_session,bold=bold),
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


   

