import argparse
import csv
import datetime
import gzip
import json
import logging
import multiprocessing
import os
import re
import requests
import sys
import urllib3

import psycopg2

import datetime 


GHARCHIVE_URL = 'http://data.githubarchive.org/{0}.json.gz'
FROM_DATE = '2017-01-04-00'
TO_DATE = '2017-01-05-00'

TARGET_EVENTS = ['IssueEvent', 'PullRequestEvent']
SCHEMA = ['username', 'repo', 'type', 'action', 'created_at']

POOL_SIZE = 4
MAX_RETRY = 10
CONNECTION_RETRY = 10



def process_archive(qarchive, qresult):
    last_path = None

    conn = psycopg2.connect(database="DB", user="USER", password="PASSWORD", host="HOST", port="PORT")

    while True:
        item = qarchive.get()
        archive_path = item[0]
        print(item[0])
        qarchive.task_done()
        if not archive_path or archive_path == last_path:
            print("d")
            break
            # continue
        if not os.path.exists(archive_path):
            logging.debug("Archive %s not found", archive_path)
            continue
        i = 0
        with gzip.open(archive_path, 'r') as content:
            activity = []
            for line in content:
                decoded = line.decode("utf-8")
                delimited = re.sub(r'}{"(?!\W)', '}JSONDELIMITER{"', decoded)
                for chunk in delimited.split('JSONDELIMITER'):
                    if len(chunk) == 0:
                        continue

                    try:
                        json_data = json.loads(chunk)
                    except Exception as e:
                        logging.error("Failed to load JSON %s in archive %s, %s",
                                        chunk, archive_path, str(e))
                        continue

                    # event_type = event['type']
                    # event_payload = event['payload']
                    # event_actor = event['actor'].get('login', '')

                    # if event_type not in TARGET_EVENTS:
                    #     continue

                    # if event_payload['action'] != 'opened':
                    #     continue

                    # event_repo = event['repo']['url']
                    # event_created_at = event['created_at']

                    # activity.append([event_actor, event_repo, event_type, event_payload['action'], event_created_at])

                    repo_name = json_data['repo']['name']
                    action = json_data['payload']['action'])
                    issue_id = json_data['payload']['issue']['id']
                    temp_title = json_data['payload']['issue']['title'].replace('\n','')
                    temp_title.replace('\'','\"')
                    issue_title = title[:250]
                    repo_owner = repo_name.split('/')[0]
                    time = datetime.strptime(json_data['payload']['issue']['updated_at'], '%Y-%m-%dT%XZ')
                    elapsed = (time, action)

                    activity.append(repo_name, action, issue_id, temp_title, issue_title, repo_owner, time, elapsed)
                    qresult.put(activity)
            
                    # print ("part 1")
        # with open("C:/Users/t-jaso/GithubAzureGuide/GHData/mycsv.csv", 'a') as csvfile:
        #     print(9)
        #     writer = csv.writer(csvfile, delimiter=',')
        #     writer.writerows(activity)
        cursor = cnx.cursor()
        query = "INSERT IGNORE INTO events " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(query, events)
        cnx.commit()
        cursor.close()

        # qarchive.task_done()
        logging.debug("Archive %s processed", archive_path)
        print("processed")
        # break



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


def process_archives(folder, from_date, to_date):
    """Process the events of the the GitHub user identified by `username` stored in the archives
    in `folder` with names between `from_date` and `to_date`.
    :param folder: the folder where to store the archives
    :param from_date: the starting date to process the archives
    :param to_date: the ending date to process the archives
    """
    logging.debug("Processing archives from %s to %s", from_date, to_date)
    print(1)
    activities = []
    qarchives = multiprocessing.JoinableQueue()
    qresult = multiprocessing.Queue()

    pool = multiprocessing.Pool(POOL_SIZE, process_archive, (qarchives, qresult,))
    current_date = from_date
    while current_date != to_date:
        formatted_date = format_date(current_date)
        archive_path = os.path.join(folder, formatted_date) + '.gz'
        qarchives.put((archive_path, None))

        # update current date
        d = datetime.datetime.strptime(current_date, "%Y-%m-%d-%H")
        d = d + datetime.timedelta(hours=1)
        current_date = d.strftime("%Y-%m-%d-%H")

    qarchives.put((None, None))
    qarchives.put((None, None))
    qarchives.put((None, None))
    qarchives.put((None, None))

    pool.close()
    print(10)
    pool.join()
    print(11)


    while not qresult.empty():
        print(13)

        res = qresult.get()
        activities.append(res)
        print(12)


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
        parser.add_argument('--output', dest='output', default="C:/Users/t-jaso/GithubAzureGuide/mycsv.csv", help='CSV file where to store the data')
        parser.add_argument('--from-date', dest='from_date', default=FROM_DATE, help="Starting date (yyyy-mm-dd-hh)")
        parser.add_argument('--to-date', dest='to_date', default=TO_DATE, help="Ending date (yyyy-mm-dd-hh)")

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
    from_date = args.from_date
    to_date = args.to_date
    output = args.output

    start_time = datetime.datetime.now()
    logging.debug("script started at: %s", start_time)

    if not os.path.exists(folder):
        os.makedirs(folder)


    if not os.path.exists(output):
        print(6)
        with open(output, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([g for g in SCHEMA])
    else:
        print(7)

    activities = process_archives(folder, from_date, to_date)
    print(8)
    with open(output, 'a') as csvfile:
        print(9)
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(activities)

    end_time = datetime.datetime.now()
    logging.debug("script ended at: %s", end_time)


if __name__ == '__main__':
    main()