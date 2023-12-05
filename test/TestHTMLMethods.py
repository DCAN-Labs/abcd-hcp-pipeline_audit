import unittest
import tempfile

import pandas as pd

from html import html_report_wf


class TestHTMLMethods(unittest.TestCase):
    def test_html_report_wf(self):
        with tempfile.TemporaryDirectory() as report_output_dir:
            print('created temporary directory', report_output_dir)
            d = {'col1': [1, 2], 'col2': [3, 4]}
            session_statuses_df = pd.DataFrame(data=d)
            html_report_wf(session_statuses_df, report_output_dir)
            print(f'report_output_dir: {report_output_dir}')


if __name__ == '__main__':
    unittest.main()
