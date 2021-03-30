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

def html_report_wf():
    for (count,subject) in enumerate(subjects_to_analyze):
        if bids_dir_bucket_name: # if bids dir is a bucket pull sessions from that subject
            pdb.set_trace()
            sessions_to_analyze = get_bids_sessions(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path + subject+'/',
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname)
        else:
            session_dirs = glob(os.path.join(args.bids_dir, subject, "ses-*"))
            sessions_to_analyze = [session_dir.split("/")[-1] for session_dir in session_dirs]   
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
# This likely needs to go somewhere
"""
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
"""