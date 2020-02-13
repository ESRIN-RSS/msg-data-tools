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

REPORT_TO_EMAILS = ['vasco.nunes@esa.int']
FILES_PER_TIMESTAMP = 114
report_html = """\
<html>
    <head><title></title></head>
    <body>
        <h1>MSG data status report</h1>
"""


def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="Check for missing MSG files inside a directory.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("root_dir", help="The root directory containing MSG data to check (contains YEAR directories)")
    parser.add_argument("--report", default=3, help="Report only on the last X days")
    parser.add_argument("--date", help="Select end date in the past for the scan. Format: YYYY/MM/DD")
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


def compose_order_link(year,month,day,tslot,fullday):
    starttime=f'{year}-{month}-{day}T{tslot[:2]}:{tslot[2:]}'
    if tslot=='2345' or fullday:
        endtime = f'{year}-{month}-{day}T23:59'
    else:
        endtime=f'{year}-{month}-{day}T{TIMESLOTS[TIMESLOTS.index(tslot)+1][:2]}:{TIMESLOTS[TIMESLOTS.index(tslot)+1][2:]}'
    # link=f'http://archive.eumetsat.int/usc/#co:;id=EO:EUM:DAT:MSG:HRSEVIRI;delm=O;form=HRITTAR;band=1,2,3,4,5,6,7,8,9,10,11,12;subl=1,1,3712,3712;comp=GZIP;med=NET;noti=1;satellite=MSG4,MSG2,MSG1,MSG3;ssbt={starttime};ssst={endtime};udsp=OPE;subSat=0;qqov=ALL;seev=0;smod=ALTHRV'
    link=f'https://archive.eumetsat.int/usc/#st:;id=EO:EUM:DAT:MSG:HRSEVIRI;delm=O;form=HRITTAR;band=1,2,3,4,5,6,7,8,9,10,11,12;subl=1,1,3712,3712;comp=NONE;med=NET;noti=1;satellite=MSG4,MSG2,MSG1,MSG3;ssbt={starttime};ssst={endtime};udsp=OPE;subSat=0;qqov=ALL;seev=0;smod=ALTHRV'
    return link


def check_msgdata(root_msg_data_dir: str, days_back: int, enddate: str,):
    """Cycle through the previous days, checking the msg data. Exclude present day."""
    missing_days = []
    missing_slots = []
    missing_days_report = []
    missing_files = 0
    if enddate:
        enddate=datetime.strptime(enddate, '%Y/%m/%d')
    else:
        enddate=datetime.today()
    global TIMESLOTS, report_html
    startdate = (enddate - timedelta(days=int(days_back))).strftime('%Y/%m/%d')
    report_html += f'\t\t\t<h2>scanned period => from {startdate} to {enddate.strftime("%Y/%m/%d")}\n</h2>'
    now = datetime.today().strftime("%Y%m%d")
    links_file = os.path.join("/data/eo/MSG_DATA/missing_slots", f"msg_missing_slots_links_{now}.txt")
    with open(links_file, "w") as l:
        for num_day in reversed(range(1, int(days_back) + 1)):
            year, month, day = (enddate - timedelta(days=num_day)).strftime('%Y %m %d').split()
            day_path = os.path.join(root_msg_data_dir, year, month, day)
            if not os.path.isdir(day_path):
                logging.info(f"Could not find day {day_path}")
                missing_days.append(year+month+day)
                missing_files = missing_files + 96*FILES_PER_TIMESTAMP
                # report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day}</td><td align="center">NOT FOUND</td></tr>\n'
                continue
            for tslot in TIMESLOTS:
                data_path = os.path.join(day_path, tslot)
                if not os.path.isdir(data_path):
                    logging.warning(f"Could not find timeslot {data_path}")
                    missing_slots.append(year + month + day + tslot)
                    missing_files = missing_files +  FILES_PER_TIMESTAMP
                    # report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day+tslot}</td><td align="center">NOT FOUND</td></tr>\n'
                    continue
                missing = check_files(data_path, f'{year+month+day+tslot}')
                logging.info(f"{year+month+day+tslot}: Missing {missing} files.")

                if missing:
                    # report_html += f'\t\t\t<tr bgcolor="#dd7060"><td>{year+month+day+tslot}</td><td align="center">{missing}</td></tr>\n'
                    orderlink=compose_order_link(year,month,day,tslot, False)
                    l.write(orderlink+"\n")
                    if 0 < missing < 3:
                        color = "yellow"
                    else:
                        color = "red"
                    missing_days_report.append(
                        f'\t\t\t<p style="color:{color}">{year+month+day+tslot}: Missing {missing} files of {str(FILES_PER_TIMESTAMP)} (<a href="{orderlink}">order</a>)</p>\n')
                    missing_files = missing_files +  missing
                else:
                    continue
                    # report_html += f'\t\t\t<tr><td>{year+month+day+tslot}</td><td align="center">OK</td></tr>\n'

        # if len(missing_days) > 0:
        perc_of_days_missing = len(missing_days)/int(days_back)*100
        print(perc_of_days_missing)
        if 0 < perc_of_days_missing < 30:
            color = "yellow"
        elif perc_of_days_missing > 30:
            color = "red"
        else:
            color = "green"
        report_html += f'\n<p style="color:{color}">{str(int(days_back)-len(missing_days))+" days found out of "+days_back+" days scanned"}</p>'
        total_missing_slots = (96*int(days_back))-(96*int(days_back)-(len(missing_days)*96+(len(missing_slots))))
        if 0 < total_missing_slots < 3:
            color = "yellow"
        elif total_missing_slots > 3:
            color = "red"
        else:
            color = "green"
        report_html += f'\n<p style="color:{color}">{str(96*int(days_back)-(len(missing_days)*96+(len(missing_slots))))+" timeslots found out of "+str(96*int(days_back))+ " expected"}</p>'
        if 0<missing_files<3:
            color = "yellow"
        elif missing_files>3:
            color = "red"
        else:
            color = "green"
        report_html += f'\n<p style="color:{color}">{str((96*int(days_back)*FILES_PER_TIMESTAMP)-missing_files)+" files found out of "+str(96*int(days_back)*FILES_PER_TIMESTAMP)+ " expected"}</p>'
        if len(missing_days)>0: report_html += f'\n<h3 style="color:red">Missing days:</h3>\n\n'
        for day in missing_days:
            year,month,dday = day[:4], day[4:6], day[-2:]
            orderlink = compose_order_link(year, month, dday, '0000', True)
            l.write(orderlink+"\n")
            report_html += f'\t\t\t<p style="color:red">{day} (<a href="{orderlink}">order</a>)</p>\n'
        if len(missing_slots) > 0: report_html += f'\n<h3 style="color:red">Missing slots:</h3>\n\n'
        for slot in missing_slots:
            year,month,day,tslot = slot[:4], slot[4:6], slot[6:8], slot[-4:]
            orderlink = compose_order_link(year, month, day, tslot, False)
            l.write(orderlink+"\n")
            report_html += f'\t\t\t<p style="color:red">{slot} (<a href="{orderlink}">order</a>)</p>\n'
        if len(missing_days_report) > 0: report_html += f'\n<h3 style="color:red">Missing files:</h3>\n'
        for files in missing_days_report: report_html += f'{files}'

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
        check_msgdata(args.root_dir, args.report, args.date)
        report_html += '\t\t\t</body>\n</html>'  # Finishing up the report text/formatting
        filename = os.path.join("/tmp/", 'checkmsgdata.log')
        with tempfile.TemporaryDirectory() as tempdir:
            with open(filename, 'w') as output:
                output.write(report_html)
                if REPORT_TO_EMAILS:
                    logging.info(f"Sending email to {', '.join(REPORT_TO_EMAILS)}")
        send_from_mutt(REPORT_TO_EMAILS, 'MSG data status report', filename)
        logging.info("Done")
