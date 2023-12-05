import os
import subprocess
import pandas as pd
from jinja2 import Environment, PackageLoader, select_autoescape


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
        if line == '' and process.poll() is not None:
            break
    if process.returncode != 0:
        raise Exception("Non zero return code: %d" % process.returncode)


def colormap(val):
    if type(val) == int:
        color = 'white'
    elif val == "NO_ABCD-HCP" or val == "failed":
        color = 'tomato'
    elif val == "NO BIDS":
        color = "darkorange"
    elif val == "pending" or val == "in process" or val == "not sure":
        color = 'gold'
    elif val == "ok":
        color = 'palegreen'
    else:
        color = 'white'
    return 'background-color: %s' % color


def html_report_wf(session_statuses_df, report_output_dir):
    env = Environment(
        loader=PackageLoader("utils"),
        autoescape=select_autoescape()
    )
    # save HTML table
    study_ses_count = len(session_statuses_df)
    pd.set_option('display.max_rows', study_ses_count)
    html = session_statuses_df.copy()
    html.reset_index(drop=True, inplace=True)
    # Set table's class="sortable" so that the script works on it
    htmlstyled = html.style. \
        applymap(colormap). \
        set_properties(**{'font-family': 'Helvetica', 'font-size': '8pt', 'text-align': 'center'}). \
        set_table_styles([{'selector': ' ', 'props': [('font-family', 'Helvetica')]}]). \
        set_table_attributes('class="sortable"'). \
        render()
    # Added string to the end of htmlstyled calling the JavaScript
    template = env.get_template("styled_template.html")
    htmlstyled = template.render(htmlstyled=htmlstyled)
    with open(os.path.join(report_output_dir, "s3_status_report.html"), "w") as fp:
        fp.write(htmlstyled)
