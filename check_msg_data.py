"""Monitor and report on the availability of MSG data in a local directory.
Each day should have 96 timestamp directories and each of those should contain 114 files.
So for each day, there should be 10944 H-0000-MSG* files.
You can use this script to simply report on missing data by email (useful for setting up via cron)
or to check (without emailing) a specific directory, and report on missing data.
"""
import argparse
import logging
import os
import tempfile
from calendar import Calendar
from datetime import datetime, timedelta
from typing import List

from send_email import send_from_mutt

REPORT_TO_EMAILS = ['rss_team@esa.int']
FILES_PER_TIMESTAMP = 114
report_html = """\
<html>
    <head><title></title></head>
    <body>
        <p>MSG data status report</p>
        <table border="1">
            <tr>
                <th>TIMESTAMP</th>
                <th>Missing files</th>
            </tr>
"""


def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="Check for missing MSG files inside a directory.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("root_dir", help="The root directory containing MSG data to check (contains YEAR directories)")
    parser.add_argument("--report", default=3, help="Report only on the last X days")
    parser.add_argument("--check", help="Instead of emailing a report, check a specific Year/Month/Day. Use format 'YYYY', or 'YYYY/MM' or 'YYYY/MM/DD'")
    return parser.parse_args()


def timeslots(minute_split=15) -> List[str]:
    """Generate list of timeslots for a 24H range."""
    tslots = []
    for hour in range(24):
        for hour_split in range(0, 60, minute_split):
            tslots.append(str(hour).zfill(2) + str(hour_split).zfill(2))
    return tslots


TIMESLOTS = timeslots()


def check_files(file_path: str, timestamp: str) -> int:
    """Check the files path for the required MSG data, report on missing
    files."""
    ok_files_count = 0
    for file in os.listdir(file_path):
        if len(file) == 61 and file.startswith('H-000-MSG') and file[46:58] == timestamp:
            ok_files_count += 1
        else:
            logging.warning(f"{os.path.join(file_path, file)} does not belong here")
    return FILES_PER_TIMESTAMP-ok_files_count


def check_msgdata(root_msg_data_dir: str, days_back: int):
    """Cycle through the previous days, checking the msg data. Exclude present day."""
    global TIMESLOTS, report_html
    for num_day in reversed(range(1, days_back + 1)):
        year, month, day = (datetime.today() - timedelta(days=num_day)).strftime('%Y %m %d').split()
        day_path = os.path.join(root_msg_data_dir, year, month, day)
        if not os.path.isdir(day_path):
            logging.info(f"Could not find day {day_path}")
            report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day}</td><td align="center">NOT FOUND</td></tr>\n'
            continue
        for tslot in TIMESLOTS:
            data_path = os.path.join(day_path, tslot)
            if not os.path.isdir(data_path):
                logging.warning(f"Could not find timeslot {data_path}")
                report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day+tslot}</td><td align="center">NOT FOUND</td></tr>\n'
                continue
            missing = check_files(data_path, f'{year+month+day+tslot}')
            logging.info(f"{year+month+day+tslot}: Missing {missing} files.")
            if missing:
                report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day+tslot}</td><td align="center">{missing}</td></tr>\n'
            else:
                report_html += f'\t\t\t<tr><td>{year+month+day+tslot}</td><td align="center">OK</td></tr>\n'


def check_year_dir(path: str):
    """Check the MSG data from this year path."""
    for month in range(1, 13):
        month_str = str(month).zfill(2)
        month_path_to_check = os.path.join(path, month_str)
        if not os.path.isdir(month_path_to_check):
            logging.warning(f"Could not find {month_path_to_check}")
            continue
        check_month_dir(month_path_to_check)


def check_month_dir(path: str):
    """Check the MSG data from this month path."""
    year, month = path[-7:-3], path[-2:]  # .../2018/12
    c = Calendar()
    for day in c.itermonthdays(int(year), int(month)):
        if day == 0:
            continue
        day_str = str(day).zfill(2)
        day_path_to_check = os.path.join(path, day_str)
        if not os.path.isdir(day_path_to_check):
            logging.warning(f"Could not find {day_path_to_check}")
            continue
        if check_day_dir(day_path_to_check):
            logging.info(f'{day_path_to_check} complete')


def check_day_dir(path: str) -> bool:
    """Check the MSG data from this day path."""
    year, month, day = path[-10:-6], path[-5:-3], path[-2:]  # .../2018/12/31
    complete = True
    for tslot in TIMESLOTS:
        data_path = os.path.join(path, tslot)
        if not os.path.isdir(data_path):
            logging.warning(f"{data_path} missing")
            complete = False
            continue
        missing = check_files(data_path, f'{year+month+day+tslot}')
        if missing:
            logging.warning(f"{year+month+day+tslot} incomplete by {missing} files")
            complete = False
    return complete

def check_missing_data(msg_root_dir: str, date_fields: List[str]):
    """Check for missing data in the given root directory, and date fields (YYYY, MM, DD)."""
    year = month = day = path_to_check = None
    if len(date_fields) == 3:
        year, month, day = date_fields
        month = month.zfill(2)
        day = day.zfill(2)
        path_to_check = os.path.join(msg_root_dir, year, month, day)
    elif len(date_fields) == 2:
        year, month = date_fields
        month = month.zfill(2)
        path_to_check = os.path.join(msg_root_dir, year, month)
    else:
        year = date_fields[0]
        path_to_check = os.path.join(msg_root_dir, year)
    if not os.path.isdir(path_to_check):
        logging.critical(f'Invalid dir {path_to_check}')
        exit(1)
    if day:
        if check_day_dir(path_to_check):
            logging.info(f'{path_to_check} complete')
    elif month:
        check_month_dir(path_to_check)
    else:
        check_year_dir(path_to_check)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s:%(levelname)s:%(message)s', level=logging.INFO)
    args = setup_cmd_args()
    if not args.root_dir.startswith('/'):
        logging.critical('root_dir parameter must be an absolute path')
        exit(1)
    if args.check:
        date_values = list(filter(None, args.check.split('/')))  # remove empty strings
        if not date_values or len(date_values) > 3:
            logging.critical('check parameter is not in valid format (YYYY/MM/DD)')
            exit(1)
        check_missing_data(args.root_dir, date_values)
        exit()
    else:
        check_msgdata(args.root_dir, args.report)
        report_html += '\t\t</table>\n\t</body>\n</html>'  # Finishing up the report text/formatting
        with tempfile.TemporaryDirectory() as tempdir:
            filename = os.path.join(tempdir, 'checkmsgdata.log')
            with open(filename, 'w') as output:
                output.write(report_html)
                if REPORT_TO_EMAILS:
                    logging.info(f"Sending email to {', '.join(REPORT_TO_EMAILS)}")
                    send_from_mutt(REPORT_TO_EMAILS, 'MSG data status report', output.name)
        logging.info("Done")
