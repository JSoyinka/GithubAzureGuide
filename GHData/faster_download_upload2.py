
import argparse
import datetime
import gzip
import hashlib
import json
import logging
import os
import queue
import re
import requests
import sys
import threading
import urllib3

import mysql.connector
from mysql.connector import errorcode

from grimoirelab_toolkit.datetime import datetime_utcnow


GHARCHIVE_URL = 'http://data.githubarchive.org/{0}.json.gz'
FROM_DATE = '2017-01-01-00'
TO_DATE = '2019-01-01-00'

ISSUES_EVENTS = 'IssuesEvent'
TARGET_EVENTS = [ISSUES_EVENTS]

POOL_SIZE = 36
MAX_RETRY = 10
CONNECTION_RETRY = 10

DB_NAME = "github_events_04082019"
DB_CONFIG = {
            'user': 'root',
            'password': '',
            'host': 'localhost',
            'port': '3306',
            'raise_on_warnings': False,
            'buffered': True
        }


class ProcessArchive(threading.Thread):
    """Process a single archive and store the obtained results to the database.
    ProcessArchive is a thread which reads from a queue and processes its items (archive paths). Each
    path is used to load the corresponding archive, its content is parsed and analysed to look for
    specific events defined in TARGET_EVENTS, the obtained data is then stored in the database.
    The thread ends as soon as it receives a None item, thus it returns and waits the other
    threads to finish.
    """
    def __init__(self, qarchive, keep_archives):
        threading.Thread.__init__(self)
        self.qarchive = qarchive
        self.keep_archives = keep_archives

    def run(self):

        def connection():
            """Create a requests.Session obj which includes a retry mechanism on connection faults"""

            session = requests.Session()

            retries = urllib3.util.Retry(total=MAX_RETRY, connect=CONNECTION_RETRY)
            adapter = requests.adapters.HTTPAdapter(max_retries=retries)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            return session

        con = connection()
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        use_database = "USE " + DB_NAME
        cursor.execute(use_database)

        while True:
            item = self.qarchive.get()

            if not item:
                logging.debug("Thread %s: exiting", self.getName())
                self.qarchive.task_done()
                break

            formatted_date = item[0]
            archive_path = item[1]

            if os.path.exists(archive_path):
                logging.debug("Thread %s: archive %s already downloaded", self.getName(), archive_path)
            else:
                url = GHARCHIVE_URL.format(formatted_date)
                try:
                    response = con.get(url, stream=True)
                    with open(archive_path, 'wb') as fd:
                        fd.write(response.raw.read())

                    logging.debug("Thread %s: archive %s downloaded", self.getName(), archive_path)

                except Exception as e:
                    logging.error("Thread %s: archive %s not collected due to %s", self.getName(), url, str(e))
                    self.qarchive.task_done()
                    continue

            if not os.path.exists(archive_path):
                logging.debug("Thread %s: archive %s not found", self.getName(), archive_path)
                self.qarchive.task_done()
                continue

            events = []
            with gzip.open(archive_path, 'r') as content:
                for line in content:
                    decoded = line.decode("utf-8")
                    delimited = re.sub(r'}{"(?!\W)', '}JSONDELIMITER{"', decoded)
                    for chunk in delimited.split('JSONDELIMITER'):
                        if len(chunk) == 0:
                            continue

                        try:
                            event = json.loads(chunk)
                        except Exception as e:
                            logging.error("Thread %s: failed to load JSON %s in archive %s, %s",
                                          self.getName(), chunk, archive_path, str(e))
                            continue

                        event_type = event['type']
                        event_payload = event['payload']
                        event_action = ''
                        commits = None
                        distinct_commits = None

                        # action attribute is in IssueEvents and PullRequestEvents
                        if event_type in [ISSUES_EVENTS, PULL_EVENTS]:
                            event_action = event_payload['action']
                            if event_action != 'opened':
                                continue
                        elif event_type == PUSH_EVENTS:
                            commits = event_payload['size']
                            distinct_commits = event_payload['distinct_size']
                        else:
                            # ignore other events
                            continue

                        event_actor = event['actor']['login']
                        event_repo = event['repo']['url']
                        event_created_at = event['created_at']

                        s = ':'.join([event_repo, event_actor, event_type, event_action, event_created_at])
                        sha1 = hashlib.sha1(s.encode('utf-8', errors='surrogateescape'))
                        uuid = sha1.hexdigest()

                        events.append((uuid, event_repo, event_actor, event_type,
                                       event_action, event_created_at, commits, distinct_commits))

            cursor = cnx.cursor()
            query = "INSERT IGNORE INTO events " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(query, events)
            cnx.commit()
            cursor.close()

            self.qarchive.task_done()
            logging.debug("Thread %s: archive %s processed", self.getName(), archive_path)

            if self.keep_archives:
                continue

            if os.path.exists(archive_path):
                os.remove(archive_path)
                logging.debug("Thread %s: archive %s deleted", self.getName(), archive_path)


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


def update_current_date(date):
    """Update a given date by one hour.
    ;param date: target date
    """
    d = datetime.datetime.strptime(date, "%Y-%m-%d-%H")
    d = d + datetime.timedelta(hours=1)
    new_date = d.strftime("%Y-%m-%d-%H")

    return new_date


def parse_archives(folder, from_date, to_date, keep_archives):
    """Download the archives from githubarchive.com generated between `from_date`
    and `to_date`. Each archive is saved to `folder`, its data is processed and the
    result stored to a database, finally the archive is deleted.
    :param folder: the folder where to store the archives
    :param from_date: the starting date to download the archives
    :param to_date: the ending date to download the archives
    :param keep_archives: if True, it keeps the archives on disk
    """
    qarchives = queue.Queue()

    # init threads
    threads = []
    for i in range(POOL_SIZE):
        t = ProcessArchive(qarchives, keep_archives)
        threads.append(t)

    # start threads
    for t in threads:
        logging.debug("Thread %s created", t.getName())
        t.start()

    current_date = from_date
    while current_date != to_date:
        formatted_date = format_date(current_date)
        archive_path = os.path.join(folder, formatted_date) + '.gz'

        qarchives.put((formatted_date, archive_path))

        current_date = update_current_date(current_date)

    # add dead pills
    for _ in threads:
        qarchives.put(None)

    # wait for all threads to finish
    for t in threads:
        t.join()


def init_database(force_init):
    """Initialize the database (if `force_init` is True, the database is recreated), which is
    composed of only the table `events` defined in the following way:
    - ID - unique ID to identify the event
    - REPO - API URL of the repo
    - ACTOR - GitHub username
    - TYPE - type of the event
    - ACTION - action of the event
    - CREATED_AT - timestamp when the action was performed
    - COMMITS - num of commits (only valid for PushEvent type)
    - DISTINCT_COMMITS - num of distinct commits (only valid for PushEvent type)
    Two indexes on action and actor are defined to speed up queries.
    :param force_init: if True deletes the previous version of the database
    """
    cnx = mysql.connector.connect(**DB_CONFIG)
    cursor = cnx.cursor()

    if force_init:
        drop_database_if_exists = "DROP DATABASE IF EXISTS " + DB_NAME
        cursor.execute(drop_database_if_exists)

    try:
        create_database = "CREATE DATABASE " + DB_NAME
        cursor.execute(create_database)
    except mysql.connector.errors.DatabaseError as e:
        logging.debug("Database not created, %s", str(e))
        pass

    cursor.execute("set global innodb_file_format = BARRACUDA")
    cursor.execute("set global innodb_file_format_max = BARRACUDA")
    cursor.execute("set global innodb_large_prefix = ON")
    cursor.execute("set global character_set_server = utf8")
    cursor.execute("set global max_connections = 500")

    use_database = "USE " + DB_NAME
    cursor.execute(use_database)

    create_table = "CREATE TABLE IF NOT EXISTS events( " \
                   "id varchar(255) PRIMARY KEY, " \
                   "repo varchar(255), " \
                   "actor varchar(255), " \
                   "type varchar(32), " \
                   "action varchar(32), " \
                   "created_at timestamp, " \
                   "commits int(5), " \
                   "distinct_commits int(5), " \
                   "INDEX actor (actor), " \
                   "INDEX repo (repo) " \
                   ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;"

    cursor.execute(create_table)
    cursor.close()


def parser(*args):
    """Parse the commands of the script."""

    def commands():
        """Define the commands of the script, which are:
        --folder: Folder to store/read the archives
        --from-date: the date to start storing/reading the archives, default FROM_DATE
        --to-date: the date to end storing/reading the archives, default TO_DATE
        --new-db: recreate the database if exists
        --keep-archives: if True, it keep the archives on disk
        """
        parser = argparse.ArgumentParser()

        parser.add_argument('--folder', dest='folder', help='Folder to store/read the GHArchive data')
        parser.add_argument('--from-date', dest='from_date', default=FROM_DATE, help="Starting date (yyyy-mm-dd-hh)")
        parser.add_argument('--to-date', dest='to_date', default=TO_DATE, help="Ending date (yyyy-mm-dd-hh)")
        parser.add_argument('--new-db', action='store_true', help="Delete the previous version of the DB if exists")
        parser.add_argument('--keep-archives', action='store_true', help="Keep archives on disk")

        return parser

    parsed_args = commands().parse_args(*args)

    return parsed_args


def main():
    """This script downloads and processes the archives from githubarchive.com
    between two dates (--from-date and --to-date) to a folder (--folder). It filters them according to
    specific GitHub events and store them in a MySQL database (`github_events`).
    """
    logging.getLogger().setLevel(logging.DEBUG)

    args = parser(sys.argv[1:])

    folder = args.folder
    from_date = args.from_date
    to_date = args.to_date
    force_init = args.new_db
    keep_archives = args.keep_archives

    start_time = datetime_utcnow().isoformat()
    logging.debug("script started at: %s", start_time)

    if not os.path.exists(folder):
        os.makedirs(folder)

    init_database(force_init)

    parse_archives(folder, from_date, to_date, keep_archives)

    end_time = datetime_utcnow().isoformat()
    logging.debug("script ended at: %s", end_time)


if __name__ == '__main__':
    main()