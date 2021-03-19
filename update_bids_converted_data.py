#!/usr/bin/env python3

import os
import subprocess

with open("hcp_slurm.conf") as conf:
    for line in conf:
        if line.startswith("bids_bucket"):
            s3_bucket = line.split('=')[1].replace('"','').replace('\n','')
        if line.startswith("bids_dir_name"):
            bids_dir_name = line.split('=')[1].replace('"','').replace('\n','')

bids_dir = os.path.join(os.path.dirname(os.getcwd()),bids_dir_name)
s3cmd = '/usr/bin/s3cmd'

print("BIDSDIR",bids_dir)
print("S3",s3_bucket)
out = subprocess.Popen([s3cmd, 'ls', s3_bucket+'/sub*'],
                       stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

stdout,stderr = out.communicate()
subj_id_folder_paths = stdout.decode('utf-8').replace("/\n",'').replace(" ","").split('DIR')[1:]

print(subj_id_folder_paths)

for subj_id_folder in subj_id_folder_paths:
    subj_id=subj_id_folder.split('/')[-1]
    if os.path.isdir(os.path.join(bids_dir,subj_id)):
        out = subprocess.Popen([s3cmd, 'ls',s3_bucket+'/'+subj_id+'/ses*'], 
        stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        stdout,stderr = out.communicate()
        ses_id_folder_paths = stdout.decode('utf-8').replace("/\n",'').replace(" ","").split('DIR')[1:]
        for ses_id_folder in ses_id_folder_paths:
            ses_id = ses_id_folder.split('/')[-1]
            if not os.path.isdir(os.path.join(bids_dir,subj_id,ses_id)):
                print('Transfer session id ' + ses_id) 
                os.system(s3cmd + ' get --recursive ' + ses_id_folder + ' '+
                  os.path.join(bids_dir,subj_id)+'/')               
    else:
        print('Transfer subject id ' + subj_id)
        os.mkdir(os.path.join(bids_dir,subj_id))
        os.system(s3cmd + ' sync --recursive --check-md5 ' + subj_id_folder + ' '+bids_dir+'/')
os.chdir('/home/cullenkr/shared/275_BRIDGES/slurm_HCP_2')
os.system('bash make_run_files.sh')
print('Run files up to date')


