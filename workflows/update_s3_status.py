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

with open("hcp_slurm.conf") as conf:
    for line in conf:
        if line.startswith("hcp_bucket"):
            bucketName = line.split('=')[1].replace('"','').replace('\n','').split('/')[2]

data_dir=os.path.abspath('..')
current_dir = os.getcwd()

# set up s3 connection
out = subprocess.Popen(['s3info', '--machine-output'],
                      stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
stdout,stderr = out.communicate()
access_key = stdout.decode('utf-8').replace("\n",' ').split(' ')[1]
secret_key = stdout.decode('utf-8').replace("\n",' ').split(' ')[2]
host='https://s3.msi.umn.edu'
s3session = boto3.session.Session()
client = s3session.client('s3',endpoint_url=host,
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)



def get_bold_processed_outputs(bucketName,access_key,secret_key,host,subject,scanning_session,bold):

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
        
    
# BIDS layout

print('\n\n')
bids_dir = os.path.join(data_dir,'BIDS_output_2')
layout = BIDSLayout(bids_dir,validate=False)
expected_tasks = []
for t in layout.get_tasks():
    if len(layout.get_acquisitions(task=t)) > 0:
        for a in layout.get_acquisitions(task=t):
            expected_tasks.append(t+'_acq-'+a)
    else:
        expected_tasks.append(t)
    
print("Found the following fMRI tasks: ", expected_tasks)
columns = expected_tasks.copy()
columns.insert(0, "structural")
columns.insert(0, "ses_id")
columns.insert(0, "subj_id")
session_statuses = pd.DataFrame(columns=columns)
study_ses_count = len(layout.get_sessions())


hopelessdf = pd.DataFrame(columns = ['ses_id','note'])
with open("hopeless.list") as hopelessfile:
    for line in hopelessfile:
        if line[0].isdigit():
             hopelessid = line.split('#')[0].strip()
             hopelessnote = line.split('#')[1].strip()
             hopelessdf = hopelessdf.append({
                    'ses_id':hopelessid,
                    'note':hopelessnote},ignore_index=True)

# Iterate through sessions

for (count,session) in enumerate(layout.get_sessions()):
    subject = layout.get_subjects(session=session)[0]
    print("...scanning session {} of {} (sub-{} ses-{})         ".format(count+1,study_ses_count,subject,session),end='\r')
    session_status = pd.DataFrame(columns=columns,index=range(1))
    session_status.loc[0].subj_id = subject
    session_status.loc[0].ses_id = session
    bolds = layout.get(session=session,subject=subject,type='bold',extensions='.nii.gz',return_type='file')
    preprocessed_bolds = []
    bids_bolds = []
    for bold in bolds:
        bold_prefix = os.path.basename(bold).split('.')[0]
        key = get_bold_processed_outputs(bucketName=bucketName,access_key=access_key,secret_key=secret_key,host=host,subject='sub-'+str(subject),scanning_session='ses-'+str(session),bold=bold_prefix)
        if key:
            preprocessed_bolds.append(key.split("task-")[1].split("_run")[0])
    for bold in bolds:
        bids_bolds.append(bold.split("task-")[1].split("_run")[0])
    s3_tasks = list(set(preprocessed_bolds))
    bids_tasks = list(set(bids_bolds))
    for task in expected_tasks:
        if task in bids_tasks and task not in s3_tasks:
            task_status="NO_HCP"
        elif task in bids_tasks and task in s3_tasks:
            task_status="ok"
        elif task not in bids_tasks:
            task_status="NO_BIDS"
        session_status.loc[0,task] = task_status
    
    
    # LOOK IN S3 FOR FINAL STRUCTURAL OUTPUT
    
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
    session_status.loc[0,'structural'] = struc_status
    session_statuses = session_statuses.append(session_status,ignore_index=True)

print('\n\n')

# save output to CSV


session_statuses = pd.merge(session_statuses,hopelessdf,how="left",on="ses_id")
session_statuses.columns = [col.replace('task-', '') for col in session_statuses.columns]
session_statuses = session_statuses.sort_values(by=['subj_id','ses_id'],ignore_index=True)
session_statuses = session_statuses.replace(np.nan, '', regex=True)
session_statuses.to_csv('s3_status_report.csv')


# save HTML table

pd.set_option('display.max_rows', study_ses_count)


def colormap(val):
    if type(val) == int:
        color='white'
    elif val == "NO_BIDS": 
        color='tomato'
    elif val == "NO_HCP": 
        color='gold'
    elif val == "ok": 
        color='palegreen'
    else:
        color='white'
    return 'background-color: %s' % color


html = session_statuses.copy()
htmlstyled = html.style.\
             applymap(colormap).\
             set_properties(**{'font-family':'Helvetica','font-size':'8pt','text-align':'center'}).\
             set_properties(subset=['note'], **{'text-align': 'left'}).\
             set_table_styles([{'selector': ' ', 'props': [('font-family', 'Helvetica')]}]).\
             render()
  
with open("s3_status_report.html","w") as fp:
   fp.write(htmlstyled)

# print output to stdout

print(session_statuses.to_string())

