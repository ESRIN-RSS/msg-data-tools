"""To sort existing MSG files into the appropriate structure, in the final storage
/data/eo/MSG_DATA/YYYY/MM/DD/timeslot."""
import argparse
import logging
import os
import re
import shutil

TARGET_ROOT_DIR = '/data/eo/MSG_DATA'  # /YYYY/MM/DD/HHMM
REGEX_TIMESLOT_DIR = re.compile(f'{TARGET_ROOT_DIR}/'+r'\d{4}/\d{2}/\d{2}/\d{4}')

def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="Sort the MSG files inside a directory")
    parser.add_argument("source_dir", help="The directory containing data to process (YEAR dir)")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s:%(levelname)s:%(message)s', level=logging.INFO)
    args = setup_cmd_args()
    logging.info("Started")

    for root, dirs, files in os.walk(args.source_dir):
        if REGEX_TIMESLOT_DIR.match(root):
            continue  # already in place, skip
        for file in files:
            if len(file) != 61:
                logging.error(f"Not a valid MSG file: {file}")
                continue
            year, month, day, time = file[46:50], file[50:52], file[52:54], file[54:58]
            if 'SERVICE' in file:
                target_dir = os.path.join(TARGET_ROOT_DIR, year, month, day)
            else:
                target_dir = os.path.join(TARGET_ROOT_DIR, year, month, day, time)
            if target_dir.endswith(root):
                continue  # already in place
            os.makedirs(target_dir, exist_ok=True)
            logging.info(f"Moving {os.path.join(root, file)} to {target_dir}")
            try:
                shutil.move(os.path.join(root, file), target_dir)
            except shutil.Error as err:
                logging.error(err)
                continue
    logging.info(f"Done processing {args.source_dir}")
