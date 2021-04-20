#!/usr/bin/sh

set +x

output_report_dir=$1
base_dir=$2
cd ${base_dir}
sed -i '1 i\
<head><script src="scripts/sorttable.js"></script></head>' ${output_report_dir}/s3_status_report.html

sed -i 's/table id/table class="sortable" id/g' ${output_report_dir}/s3_status_report.html
