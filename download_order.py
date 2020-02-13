"""Easily download all the tar files from an EUMETSAT order,
even if they are spread across multiple web-pages."""
import argparse
import os
import queue
import subprocess
from threading import Thread

import requests
from lxml import html

CREDENTIALS = ('sseteam', 'DsLWSecG')
DOWNLOAD_WORKER_THREADS = 5
download_queue = queue.Queue()


def verify_local_files(path: str):
    """Verify the integrity of .tar files in the given path

    :param path: path with tar files to check for errors
    :return: tuple with list of good files, and files with errors
    """
    files_ok = []
    files_errors = []
    for file in os.listdir(path):
        if file.endswith('.tar'):
            check_tar = subprocess.run(['tar', '-tf', os.path.join(path, file)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if check_tar.returncode == 0:
                files_ok.append(file)
            else:
                files_errors.append(file)
    return files_ok, files_errors


def download_file(queue_files: queue.Queue):
    """Function called by daemon threads to download files

    :param queue_files: Queue object containing files to download.
    """
    while not queue_files.empty():
        file_to_download = queue_files.get()
        print("Downloading", file_to_download)
        #subprocess.call(['wget', '--retry-connrefused', '--waitretry', '1', '--tries', '5', '--continue',
        #                '--http-user', CREDENTIALS[0], '--http-password', CREDENTIALS[1], '-O',
        #                os.path.join(args.ordernum, file_to_download.split('/')[-1]), file_to_download],
        #               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.call(['curl','-u', CREDENTIALS[0]+':'+CREDENTIALS[1], '-o',
                        os.path.join(args.ordernum, file_to_download.split('/')[-1]), file_to_download],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and verify products from the EUMETSAT archive.')
    parser.add_argument('ordernum', help='Number of the EUMETSAT order')
    parser.add_argument('--verify', action='store_true',
                        help='Only perform verification of the downloaded tar files in the order directory')
    args = parser.parse_args()

    if args.verify:
        if not os.path.exists(args.verify):
            print("Could not find path", args.verify)
            exit(-1)
        ok, errors = verify_local_files(args.ordernum)
        if not ok:
            print("No valid tar files found!")
        else:
            if errors:
                print("Errors in: ", ' '.join(errors))
            else:
                print("No errors found")
        exit()

    os.makedirs(args.ordernum, exist_ok=True)
    page = requests.get('http://archive.eumetsat.int/usc/onlinedownload/sseteam/' + args.ordernum + '/',
                        auth=CREDENTIALS)
    web_page = html.fromstring(page.content)
    web_page.make_links_absolute('http://archive.eumetsat.int/usc/onlinedownload/sseteam/' + args.ordernum + '/')

    links = web_page.xpath('//a/@href')
    tar_files = []
    print("Obtaining product links from EUMETSAT")
    for l in links:
        if l.endswith('.htm'):
            sub_page = requests.get(l, auth=CREDENTIALS)
            sub_web_page = html.fromstring(sub_page.content)
            sub_web_page.make_links_absolute(l)
            sub_links = sub_web_page.xpath('//a/@href')
            for link in sub_links:
                if link.endswith('.tar'):
                    tar_files.append(link)
        elif l.endswith('.tar'):
            tar_files.append(l)

    if not tar_files:
        print("Could not find any tar files to download")
        exit()

    ok_files, _ = verify_local_files(args.ordernum)
    for url_tar in tar_files:
        if url_tar.split('/')[-1] not in ok_files:
            download_queue.put(url_tar)

    if download_queue.empty():
        print("Nothing to download.", args.ordernum, "is complete.")
        exit()
    else:
        print(download_queue.qsize(), "files to download")

    threads = []
    for _ in range(DOWNLOAD_WORKER_THREADS):
        worker = Thread(target=download_file, args=(download_queue,))
        threads.append(worker)
        worker.start()
    for t in threads:
        t.join()
    print("DONE")
