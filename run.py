#!/usr/bin/env python3

# Import packages
import argparse
import os
import subprocess
#import nibabel
import numpy
from glob import glob
import pandas
import boto3
import botocore

# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
from multiprocessing import Pool
from itertools import product

#debugging
import pdb

__version__ = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'version')).read()

def run(command, env={}):
    merged_env = os.environ
    merged_env.update(env)
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, shell=True,
                               env=merged_env)
    while True:
        line = process.stdout.readline()
        line = str(line, 'utf-8')[:-1]
        print(line)
        if line == '' and process.poll() != None:
            break
    if process.returncode != 0:
        raise Exception("Non zero return code: %d"%process.returncode)

def get_bids_subjects(access_key,bucketName,prefix,secret_key,host):
    pdb.set_trace()
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

def get_bids_sessions(subject_id_path,bucketName,access_key,secret_key,host):
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host,
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key)
    
    get_bids_sessions = client.list_objects_v2(Bucket=bucketName,Delimiter='/',EncodingType='url',
                                          MaxKeys=1000,
                                          Prefix='%s/' %(subject_id_path),
                                          ContinuationToken='',
                                          FetchOwner=False,
                                          StartAfter='')
    bids_sessions = [item['Prefix'].split('/')[1] for item in get_bids_sessions['CommonPrefixes']]
    return bids_sessions

parser = argparse.ArgumentParser(description='abcd-hcp-pipeline_audit ntrypoint script.')
parser.add_argument('bids_dir', help='The directory with the input dataset '
                    'formatted according to the BIDS standard. In the case that the BIDS dataset is within s3 provide the path to the folder along with "s3://BUCKET_NAME/path_to_BIDS_folder".')
parser.add_argument('output_dir', help='The directory where the output files '
                    'are stored. If you are running group level analysis '
                    'this folder should be prepopulated with the results of the'
                    'participant level analysis.In the case that this folderis within s3 provide the path to the folder along with "s3://BUCKET_NAME/path_to_derivatives_folder".')
parser.add_argument('analysis_level', help='Level of the analysis that will be performed. '
                    'Unless checking on status of one participant''s processing, use "group".',
                    choices=['participant', 'group'])
parser.add_argument('--participant_label', '--participant-label',help='The label(s) of the participant(s) that should be analyzed. The label '
                   'corresponds to sub-<participant_label> from the BIDS spec '
                   '(so it does not include "sub-"). If this parameter is not '
                   'provided all subjects should be analyzed. Multiple '
                   'participants can be specified with a space separated list.',
                   nargs="+")
parser.add_argument('--n_cpus',required=False,help='Number of CPUs to use for parallel download.',type=int)
parser.add_argument('--s3_access_key',required=False,type=str,
                        help='Your S3 access key, if data is within S3. If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials')
parser.add_argument('--s3_hostname',required=False,default='https://s3.msi.umn.edu',type=str,
                        help='URL for S3 storage hostname, if data is within S3 bucket. Defaults to s3.msi.umn.edu for MSIs tier 2 CEPH storage.')
parser.add_argument('--s3_secret_key',required=False,type=str,
                        help='Your S3 secret key. If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials')                        
parser.add_argument('--skip_bids_validator', help='Whether or not to perform BIDS dataset validation',
                   action='store_true')
parser.add_argument('--session_label', help='The label(s) of the session(s) that should be analyzed. The label '
                   'corresponds to ses-<session_label> from the BIDS spec '
                   '(so it does not include "sub-"). If this parameter is not '
                   'provided all subjects should be analyzed. Multiple '
                   'participants can be specified with a space separated list.',
                   nargs="+")
parser.add_argument('-v', '--version', action='version',
                    version='abcd-hcp-pipeline_audit version {}'.format(__version__))

# Parse and gather arguments
args = parser.parse_args()

# determine if bids_dir or output_dir are S3 buckets, and their respective names if so.
if 's3://' in args.bids_dir or 's3://' in args.output_dir:
    # set up s3 connection
    assert args.s3_access_key, print(args.bids_dir + ' or ' +  args.output_dir + ' are S3 buckets but you did not input a S3 access key following argument "--s3_access_key". If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials.')
    assert args.s3_secret_key, print(args.bids_dir + ' or ' +  args.output_dir + ' are S3 buckets but you did not input a S3 secret key following argument "--s3_secret_key". If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials.')
    if 's3://' in args.bids_dir:
        bids_dir_bucket_name = args.bids_dir.split('s3://')[1].split('/')[0]
        bids_dir_relative_path = args.bids_dir.split('s3://'+bids_dir_bucket_name)[1]
        if bids_dir_relative_path == '/': 
            bids_dir_relative_path = ''
    else:
        bids_dir_bucket_name = ''
    if 's3://' in args.output_dir:
        output_dir_bucket_name = args.output_dir.split('s3://')[1].split('/')[0]
        output_dir_relative_path = args.output_dir.split('s3://'+output_dir_bucket_name)[1]
        if output_dir_relative_path == '/':
            output_dir_relative_path = ''
    else:
        output_dir_bucket_name = ''

subjects_to_analyze = []
# only for a subset of subjects
if args.participant_label:
    subjects_to_analyze = args.participant_label
# for all subjects
else:
    if bids_dir_bucket_name:
        pdb.set_trace()
        subjects_to_analyze = get_bids_subjects(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path,
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname)
    else:
        subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
        subjects_to_analyze = [subject_dir.split("-")[-1] for subject_dir in subject_dirs]

# running participant level
if args.analysis_level == "participant":
    pass

# running group level
elif args.analysis_level == "group":
    pass
