#!/usr/bin/env python3

import argparse
import os
import subprocess
from glob import glob
from utils.bids import s3_get_bids_subjects, s3_get_bids_sessions, s3_abcd_hcp_struct_outputs, s3_abcd_hcp_func_outputs, s3_get_bids_funcs

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

parser = argparse.ArgumentParser(description='abcd-hcp-pipeline_audit entrypoint script.')
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

# only for a subset of subjects at participant level
if args.participant_label and args.analysis_level == "participant":
    subjects_to_analyze = args.participant_label
# running group level for all subject
elif args.analysis_level == "group":
    if bids_dir_bucket_name:
        subjects_to_analyze = s3_get_bids_subjects(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path,
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname)

    else:
        subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
        subjects_to_analyze = [subject_dir.split("/")[-1] for subject_dir in subject_dirs]
else:
    raise Exception("You must enter participant --participant_label or group in order to run.")

# some prelimnaries prior to looping through data
if bids_dir_bucket_name:
    if not 'sub-' in subjects_to_analyze[0]:
        subjects_to_analyze[0] = 'sub-'+subjects_to_analyze[0]
    sessions_to_analyze = s3_get_bids_sessions(bucketName=bids_dir_bucket_name, 
                        prefix=bids_dir_relative_path + subjects_to_analyze[0]+'/',
                        access_key=args.s3_access_key, 
                        secret_key=args.s3_secret_key, 
                        host=args.s3_hostname)
    #columns.insert(0, "structural")
    #columns.insert(0, "subj_id")
    expected_tasks = []
    if len(sessions_to_analyze) > 0:
        #columns.insert(0, "ses_id")
        pdb.set_trace()
        funcs = s3_get_bids_funcs(access_key=args.s3_access_key,
            bucketName=bids_dir_bucket_name,
            secret_key=args.s3_secret_key, 
            host=args.s3_hostname,)
            #prefix=bids_dir_relative_path + subjects_to_analyze[0]+'/'+sessions_to_analyze[0]+'/func/')
        pdb.set_trace()
    else:
        funcs = s3_get_bids_funcs(access_key=args.s3_access_key,
            bucketName=bids_dir_bucket_name,
            secret_key=args.s3_secret_key, 
            host=args.s3_hostname,
            prefix=bids_dir_relative_path + subjects_to_analyze[0]+'/'+'func')
else:
    #  bids layout and expected tasks
    layout = BIDSLayout(args.bids_dir,validate=False)
    expected_tasks = []
    for t in layout.get_tasks():
        if len(layout.get_acquisitions(task=t)) > 0:
            for a in layout.get_acquisitions(task=t):
                expected_tasks.append(t+'_acq-'+a)
        else:
            expected_tasks.append(t)
    columns = expected_tasks.copy()
    columns.insert(0, "structural")
    columns.insert(0, "subj_id")
    if len(layout.get_sessions()) > 0:
        columns.insert(0, "ses_id")
    statuses = pd.DataFrame(columns=columns)

for subject in subjects_to_analyze:
    if bids_dir_bucket_name: # if bids dir is a bucket pull sessions from that subject
        sessions_to_analyze = s3_get_bids_sessions(bucketName=bids_dir_bucket_name, 
                        prefix=bids_dir_relative_path + subject+'/',
                        access_key=args.s3_access_key, 
                        secret_key=args.s3_secret_key, 
                        host=args.s3_hostname)
    else:
        session_dirs = glob(os.path.join(args.bids_dir, subject, "ses-*"))
        sessions_to_analyze = [session_dir.split("/")[-1] for session_dir in session_dirs]
        
    if session_dirs:
        if bids_dir_bucket_name:
            pass
        else:
            print("Found the following fMRI tasks: ", expected_tasks)
            
            study_ses_count = len(layout.get_sessions())
        for session in sessions_to_analyze:
            print("...scanning session {} (sub-{} ses-{})         ".format(count+1,subject,session),end='\r')
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
            session_status.loc[0,'structural'] = struc_status
            session_statuses = session_statuses.append(session_status,ignore_index=True)

            # save output to CSV
            session_statuses = pd.merge(session_statuses,hopelessdf,how="left",on="ses_id")
            session_statuses.columns = [col.replace('task-', '') for col in session_statuses.columns]
            session_statuses = session_statuses.sort_values(by=['subj_id','ses_id'],ignore_index=True)
            session_statuses = session_statuses.replace(np.nan, '', regex=True)
            session_statuses.to_csv('s3_status_report.csv')

        print('\n\n')
    else:
        if bids_dir_bucket_name:
            pass
        else:
            print("Found the following fMRI tasks: ", expected_tasks)
            columns = expected_tasks.copy()
            columns.insert(0, "structural")
            columns.insert(0, "subj_id")
            session_statuses = pd.DataFrame(columns=columns)
            study_ses_count = len(layout.get_subjects())


        
