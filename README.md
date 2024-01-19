# abcd-hcp-pipeline_audit

This audit is used to check on the status of jobs running the ABCD-HCP pipeline. It takes in a BIDS valid input folder, and an output folder where the pipeline outputs are stored. It compares them to determine if the pipeline produced the expected outputs and if they are BIDS valid. It will produce a csv and html file with each subject/session from the input folder and a column for each of the runs it found, filled with 'ok', 'NO ABCD-HCP-PIPELINE', or 'NO BIDS'. Input and output folders can be either s3 buckets or on Tier 1 storage. If you are using data on the s3, you need to provide the s3 access and secret keys.

## Usage
```
usage: run.py [-h] --report_output_dir REPORT_OUTPUT_DIR
              [--participant_label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]]
              [--n_cpus N_CPUS] [--s3_access_key S3_ACCESS_KEY]
              [--s3_hostname S3_HOSTNAME] [--s3_secret_key S3_SECRET_KEY]
              [--skip_bids_validator]
              [--session_label SESSION_LABEL [SESSION_LABEL ...]] [-v]
              bids_dir output_dir {participant,group}

abcd-hcp-pipeline_audit entrypoint script.

positional arguments:
  bids_dir              The directory with the input dataset formatted
                        according to the BIDS standard. In the case that the
                        BIDS dataset is within s3 provide the path to the
                        folder along with
                        "s3://BUCKET_NAME/path_to_BIDS_folder".
  output_dir            The directory where the output files are stored. If
                        you are running group level analysis this folder
                        should be prepopulated with the results of
                        theparticipant level analysis.In the case that this
                        folderis within s3 provide the path to the folder
                        along with
                        "s3://BUCKET_NAME/path_to_derivatives_folder".
  {participant,group}   Level of the analysis that will be performed. Unless
                        checking on status of one participants processing, use
                        "group".

optional arguments:
  -h, --help            show this help message and exit
  --report_output_dir REPORT_OUTPUT_DIR, --report-output-dir REPORT_OUTPUT_DIR
                        The directory where the CSV and HTML files will be
                        outputted once the report finishes.
  --participant_label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...], --participant-label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]
                        The label(s) of the participant(s) that should be
                        analyzed. The label corresponds to
                        sub-<participant_label> from the BIDS spec (so it does
                        not include "sub-"). If this parameter is not provided
                        all subjects should be analyzed. Multiple participants
                        can be specified with a space separated list.
  --n_cpus N_CPUS       Number of CPUs to use for parallel download.
  --s3_access_key S3_ACCESS_KEY
                        Your S3 access key, if data is within S3. If using
                        MSI, this can be found at:
                        https://www.msi.umn.edu/content/s3-credentials
  --s3_hostname S3_HOSTNAME
                        URL for S3 storage hostname, if data is within S3
                        bucket. Defaults to s3.msi.umn.edu for MSIs tier 2
                        CEPH storage.
  --s3_secret_key S3_SECRET_KEY
                        Your S3 secret key. If using MSI, this can be found
                        at: https://www.msi.umn.edu/content/s3-credentials
  --skip_bids_validator
                        Whether or not to perform BIDS dataset validation
  --session_label SESSION_LABEL [SESSION_LABEL ...]
                        The label(s) of the session(s) that should be
                        analyzed. The label corresponds to ses-<session_label>
                        from the BIDS spec (so it does not include "sub-"). If
                        this parameter is not provided all subjects should be
                        analyzed. Multiple participants can be specified with
                        a space separated list.
  -v, --version         show program's version number and exit

```
## Merging Multiple Outputs

If you need to run the audit on more than one output bucket, therefore creating multiple status.csv files, you can use the `concat_s3_status.py` script to merge them together. This script will merge the multiple csvs into one output csv without any duplicates (unless the `--keep-duplicate-ids` flag is specified). If you want to keep track of where each subject originated from, you can specify the `src_csv` flag which will add a column that contains the path to which input csv that row came from. You can specify the `last-ok-col` flag to add a column that contains the last column in a row that has "ok" as the value. 

## Usage

usage: merge_s3.py [-h] -i INPUT [INPUT ...] -o OUTPUT [--last-ok-col]
                   [--src-csv] [--keep-duplicate-ids]

Process CSV files and create output CSV.

required arguments:
  -i INPUT [INPUT ...], --input INPUT [INPUT ...]
                        Paths to input CSV files
  -o OUTPUT, --output OUTPUT
                        Path to the output CSV file

optional arguments:
  -h, --help            show this help message and exit
  --last-ok-col         Include 'last_ok_col' column in the output
  --src-csv             Include 'src_csv' column in the output
  --keep-duplicate-ids  Keep duplicate rows based on 'subj_id'
