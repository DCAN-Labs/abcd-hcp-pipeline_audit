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
