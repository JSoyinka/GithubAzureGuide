

import argparse
import csv
import datetime
import gzip
import json
import logging
import os
import queue
import re
import requests
import sys
import threading
import urllib3


import datetime


GHARCHIVE_URL = 'http://data.githubarchive.org/{0}.json.gz'
FROM_DATE = '2017-01-01-00'
TO_DATE = '2017-01-03-00'

TARGET_EVENTS = ['IssueEvent', 'PullRequestEvent']
SCHEMA = ['username', 'repo', 'type', 'action', 'created_at']

POOL_SIZE = 8
MAX_RETRY = 10
CONNECTION_RETRY = 10


class ProcessArchive(threading.Thread):

    def __init__(self, qarchive, qresult):
        threading.Thread.__init__(self)
        self.qarchive = qarchive
        self.qresult = qresult

    def run(self):

        while True:
            item = self.qarchive.get()

            archive_path = item[0]
            username = item[1]

            if not archive_path and not username:
                logging.debug("Thread %s exiting", self.getName())
                self.qarchive.task_done()
                break

            if not os.path.exists(archive_path):
                logging.debug("Archive %s not found", archive_path)
                self.qarchive.task_done()
                continue

            with gzip.open(archive_path, 'r') as content:
                for line in content:
                    decoded = line.decode("utf-8")
                    delimited = re.sub(r'}{"(?!\W)', '}JSONDELIMITER{"', decoded)
                    for chunk in delimited.split('JSONDELIMITER'):
                        if not chunk:
                            continue

                        try:
                            event = json.loads(chunk)
                        except Exception as e:
                            logging.error("Failed to load JSON %s in archive %s, %s",
                                          chunk, archive_path, str(e))
                            continue

                        event_type = event['type']
                        event_payload = event['payload']
                        event_actor = event['actor'].get('login', '')

                        if event_actor != username:
                            continue

                        if event_type not in TARGET_EVENTS:
                            continue

                        if event_payload['action'] != 'opened':
                            continue

                        event_repo = event['repo']['url']
                        event_created_at = event['created_at']

                        activity = [username, event_repo, event_type, event_payload['action'], event_created_at]
                        self.qresult.put(activity)

            self.qarchive.task_done()
            logging.debug("Archive %s processed by %s", archive_path, self.getName())


def connection():
    """Create a requests.Session obj which includes a retry mechanism on connection faults"""

    con = requests.Session()

    retries = urllib3.util.Retry(total=MAX_RETRY, connect=CONNECTION_RETRY)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    con.mount('http://', adapter)
    con.mount('https://', adapter)

    return con


def format_date(date):
    """Format the date to be able to query the archives hosted on githubarchive.com.
    :param date: a Datetime obj
    """
    strp_date = datetime.datetime.strptime(date, "%Y-%m-%d-%H")
    output_format = re.sub(r'-[0-9][0-9]$', '', date)
    if strp_date.hour == 00:
        output_format += '-0'
    else:
        output_format += '-' + str(strp_date.hour).lstrip('0')

    return output_format


def download_archives(folder, from_date, to_date):
    """Download the archives from githubarchive.com generated between `from_date`
    and `to_date`, and store them to `folder`.
    :param folder: the folder where to store the archives
    :param from_date: the starting date to download the archives
    :param to_date: the ending date to download the archives
    """
    con = connection()
    current_date = from_date
    while current_date != to_date:
        formatted_date = format_date(current_date)

        archive_path = os.path.join(folder, formatted_date) + '.gz'
        if os.path.exists(archive_path):
            logging.debug("Archive %s already downloaded", archive_path)
            continue

        url = GHARCHIVE_URL.format(formatted_date)
        try:
            response = con.get(url, stream=True)
            with open(archive_path, 'wb') as fd:
                fd.write(response.raw.read())

            logging.debug("Archive %s downloaded", archive_path)

        except Exception as e:
            logging.error('Archive %s not collected due to %s', url, str(e))
            continue

        # update current date
        d = datetime.datetime.strptime(current_date, "%Y-%m-%d-%H")
        d = d + datetime.timedelta(hours=1)
        current_date = d.strftime("%Y-%m-%d-%H")


def process_archives(folder, from_date, to_date, username):
    """Process the events of the the GitHub user identified by `username` stored in the archives
    in `folder` with names between `from_date` and `to_date`.
    :param folder: the folder where to store the archives
    :param from_date: the starting date to process the archives
    :param to_date: the ending date to process the archives
    :param username: target GitHub username
    """
    logging.debug("Processing archives for username %s", username)

    activities = []
    qarchives = queue.Queue()
    qresult = queue.Queue()

    # init threads
    threads = []
    for i in range(POOL_SIZE):
        t = ProcessArchive(qarchives, qresult)
        threads.append(t)

    # start threads
    for t in threads:
        logging.debug("Thread %s created", t.getName())
        t.start()

    current_date = from_date
    while current_date != to_date:
        formatted_date = format_date(current_date)
        archive_path = os.path.join(folder, formatted_date) + '.gz'
        qarchives.put((archive_path, username))

        # update current date
        d = datetime.datetime.strptime(current_date, "%Y-%m-%d-%H")
        d = d + datetime.timedelta(hours=1)
        current_date = d.strftime("%Y-%m-%d-%H")

    # add dead pills
    for _ in threads:
        qarchives.put((None, None))

    # wait for all threads to finish
    for t in threads:
        t.join()

    # consume the results
    while not qresult.empty():
        res = qresult.get()
        activities.append(res)

    return activities


def parser(*args):
    """Parse the commands of the script."""

    def commands():
        """Define the commands of the script, which are:
        --folder: Folder to store/read the archives
        --download: if True, it downloads the archives, default False
        --output: a csv to store the actions related to a set of GitHub users
        --from-date: the date to start storing/reading the archives, default FROM_DATE
        --to-date: the date to end storing/reading the archives, default TO_DATE
        --usernames: a file contaning a list of GitHub usernames (one per line)
        """
        parser = argparse.ArgumentParser()

        parser.add_argument('--folder', dest='folder', default=os.getcwd(), help='Folder to store/read the GHArchive data')
        parser.add_argument('--download', action='store_true', default=True, help='Download GHArchive data')
        parser.add_argument('--output', dest='output', default="temp.csv", help='CSV file where to store the data')
        parser.add_argument('--from-date', dest='from_date', default=FROM_DATE, help="Starting date (yyyy-mm-dd-hh)")
        parser.add_argument('--to-date', dest='to_date', default=TO_DATE, help="Ending date (yyyy-mm-dd-hh)")
        parser.add_argument('--usernames', dest='usernames', default="usernames.csv", help="File containing the GitHub usernames")

        return parser

    parsed_args = commands().parse_args(*args)

    return parsed_args


def main():
    """This script downloads and processes the archives from githubarchive.com
    between two dates (--from-date and --to-date) to a folder (--folder). It returns a CSV
    file (--output), which contains the pull requests and issues opened by a set of GitHub users
    (included in the file --usernames).
    """
    logging.getLogger().setLevel(logging.DEBUG)

    args = parser(sys.argv[1:])

    folder = args.folder
    download = args.download
    from_date = args.from_date
    to_date = args.to_date
    usernames = args.usernames
    output = args.output

    start_time = datetime.datetime.now()
    logging.debug("script started at: %s", start_time)

    if not os.path.exists(folder):
        os.makedirs(folder)

    if download:
        download_archives(folder, from_date, to_date)

    if not os.path.exists(output):
        with open(output, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([g for g in SCHEMA])

    with open(usernames, 'r') as content:
        for line in content:
            activities = process_archives(folder, from_date, to_date, line.strip())

            with open(output, 'a') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerows(activities)

    end_time = datetime.datetime.now()
    logging.debug("script ended at: %s", end_time)


if __name__ == '__main__':
    main()