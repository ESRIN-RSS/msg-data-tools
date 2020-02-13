# pylint: disable=C0103
"""Extract and move MSG data files that have been downloaded from an EUMETSAT order.
Invoke the script passing as argument the directory with the eumetsat .tar files.
The script performs the following:
- Get a list of the files inside the folder
- If there are files named like "H-000-MSG...." length 61: move && cleanup
- Elseif there are any files named "MSG....tar.gz" length 79: extract_MSG_TGZs && move && cleanup
- Elseif there are any ORDER-{}of{}.tar files: extract_order_TARs && extract_MSG_TGZs && move && cleanup
- In the end, cleanup

MOVE: Move the 'H-000-MSG...' files to /data/eo/MSG_DATA/YYYY/MM/DD/timeslot
EXTRACT_MSG_TGZs: Extract all 'MSG...tar.gz' to the order folder
EXTRACT_ORDER_TARs: Extract all 'ORDER-XofY.tar' to the order folder
CLEANUP: rename the order folder to ORDER.done
"""

import argparse
import logging
import os
import shutil
import sys
import tarfile

TARGET_DIR = '/data/eo/MSG_DATA/'
parser = argparse.ArgumentParser(description="Process a MSG data order folder")
parser.add_argument("order_folder", help="The order folder to process")
args = parser.parse_args()
os.chdir(args.order_folder)

# setup logging
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
fileHandler = logging.FileHandler('{}-done.log'.format(os.path.relpath('.', '..')))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


def move_msg_files(files=None):
    if not files:
        files = sorted([f for f in os.listdir('.') if os.path.isfile(f) and f.startswith('H-000-MSG') and len(f) == 61])
    for f in files:
        to_dir = os.path.join(TARGET_DIR, f[46:50], f[50:52], f[52:54], f[54:58])
        logger.info(f"Moving {f} to {to_dir}")
        os.makedirs(to_dir, exist_ok=True)
        try:
            shutil.move(f, os.path.join(to_dir, f))
        except shutil.Error as err:
            logging.error(err)
            continue


def extract_msg_files(files=None):
    if not files:
        files = sorted([f for f in os.listdir('.') if os.path.isfile(f) and f.startswith('MSG') and len(f) >= 79])
    for f in files:
        logger.info("Extracting %s", f)
        if f.endswith('.tar.gz'):
            tar = tarfile.open(f, 'r:gz')
        elif f.endswith('.tar.bz2'):
            tar = tarfile.open(f, 'r:bz2')
        else:
            logger.error('Unsupported file: %s', f)
            exit(-1)
        tar.extractall()
        tar.close()
        move_msg_files()
        os.remove(f)


def extract_order_files(files):
    for f in files:
        logger.info("Extracting %s", f)
        tar = tarfile.open(f, 'r:')
        tar.extractall()
        tar.close()
        extract_msg_files()


def cleanup():
    # Delete any remaining H-000 or MSG..tar.gz files
    for f in os.listdir('.'):
        if os.path.isfile(f) and ((f.startswith('H-000-MSG') and len(f) == 61) or (f.startswith('MSG') and len(f) >= 79)):
            os.remove(f)
    base_folder = os.path.abspath('..')
    order_folder = os.path.abspath('.')
    new_order_folder = os.path.join(base_folder, order_folder + '.done')
    logger.info("Renaming %s to %s", order_folder, new_order_folder)
    os.rename(order_folder, new_order_folder)
    logger.info("Done")


file_list = os.listdir('.')
msg_files = sorted([f for f in file_list if os.path.isfile(f) and f.startswith('H-000-MSG') and len(f) == 61])
msg_tar_gz_files = sorted([f for f in file_list if os.path.isfile(f) and f.startswith('MSG') and len(f) >= 79])
cur_dir_name = os.path.relpath('.', '..')
order_tar_files = sorted([f for f in file_list if os.path.isfile(f) and f.startswith(cur_dir_name) and f.endswith('.tar')])


if msg_files:
    logger.info("Found MSG extracted files")
    move_msg_files(msg_files)
elif msg_tar_gz_files:
    logger.info("Found MSG compressed files")
    extract_msg_files(msg_tar_gz_files)
elif order_tar_files:
    logger.info("Found order compressed files")
    extract_order_files(order_tar_files)
cleanup()
