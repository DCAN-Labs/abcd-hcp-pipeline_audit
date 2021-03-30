#!/usr/bin/sh

sed -i '1 i\
<head><script src="scripts/sorttable.js"></script></head>' s3_status_report.html

sed -i 's/table id/table class="sortable" id/g' s3_status_report.html
