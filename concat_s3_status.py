import pandas as pd
import argparse
import sys
import os

'''
Thomas Madison
14 August 2023
Merges together multiple status csv files
'''

def find_last_ok_column(row):
    last_ok_col = -1
    for i, value in enumerate(row[3:], start=3):
        if value == 'ok':
            last_ok_col = i
    return last_ok_col

def process_csv(input_files, output_file, include_last_ok_col, include_src_csv, keep_duplicate_ids):
    dfs = []

    for input_file in input_files:
        df = pd.read_csv(input_file)
        # Add a new column "src_csv" with the input file path for each row
        df['src_csv'] = os.path.abspath(input_file)
        
        # Add a new column "last_ok_col" with the greatest column index with 'ok'
        df['last_ok_col'] = df.apply(find_last_ok_column, axis=1)
        
        dfs.append(df)
        
    # Concatenate DataFrames from all input files
    concatenated_df = pd.concat(dfs, ignore_index=False)
    
    # Drop duplicate rows based on 'subj_id', keeping the one with the greatest 'last_ok_col'
    if not keep_duplicate_ids:
        final_df = concatenated_df.sort_values(by=['subj_id', 'last_ok_col'], ascending=[True, False])
        final_df = final_df.drop_duplicates(subset='subj_id', keep='first')
    else:
        final_df = concatenated_df

    # Get the header for output CSV
    output_header=dfs[0].columns.tolist()
    output_header[0] =''

    # Drop columns if specified by command-line flags
    if not include_last_ok_col:
        final_df = final_df.drop(columns=['last_ok_col'])
        output_header.remove('last_ok_col')
    if not include_src_csv:
        final_df = final_df.drop(columns=['src_csv'])
        output_header.remove('src_csv')
    

    # Save to output CSV with the header of the first input file
    final_df.to_csv(output_file, index=False, header=output_header)

    print("Processing completed and saved to", output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CSV files and create output CSV.")
    parser.add_argument("-i", "--input", type=str, required=True, nargs="+", help="Paths to input CSV files")
    parser.add_argument("-o", "--output", type=str, required=True, help="Path to the output CSV file")
    parser.add_argument("--last-ok-col", action="store_true", help="Include 'last_ok_col' column in the output")
    parser.add_argument("--src-csv", action="store_true", help="Include 'src_csv' column in the output")
    parser.add_argument("--keep-duplicate-ids", action="store_true", help="Keep duplicate rows based on 'subj_id'")
    
    args = parser.parse_args()

    if len(args.input) == 0:
        print("At least one input file must be specified.")
    elif len(args.input) == 1:
        print("Exactly one output file must be specified.")
    else:
        # Filter out non-existent input files
        valid_input_paths = [path for path in args.input if os.path.exists(path)]
        
        if len(valid_input_paths) == 0:
            print("No valid input CSV files found.")
        else:
            process_csv(valid_input_paths, args.output, args.last_ok_col, args.src_csv, args.keep_duplicate_ids)