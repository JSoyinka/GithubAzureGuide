import logging

import azure.functions as func

import uuid
import concurrent

import conftest
import psycopg2

from azure.servicebus import ServiceBusClient, Message
from azure.servicebus.control_client import ServiceBusService, Topic, Rule, DEFAULT_RULE_NAME
from azure.servicebus.common.constants import NEXT_AVAILABLE
from azure.servicebus.common.errors import NoActiveSession
from faster_download_upload3 import faster_download_upload_3


def message_processing(queue_client, messages):
    last_message = 0
    conn = psycopg2.connect(database="DB", user="USER", password="PASSWORD", host="HOST", port="PORT")
    while True:
        try:
            with queue_client.get_receiver(session=NEXT_AVAILABLE, idle_timeout=1) as session:
                session.set_session_state("OPEN")
                for message in session:
                    # message
                    # message[0][0]
                    # message.header.time_to_live
                    # message.sequence_number
                    # message.enqueue_sequence_number
                    # message.partition_id
                    # message.partition_key
                    # message.locked_until
                    # message.lock_token
                    # message.enqueued_time
                    # Define as the summarizer
                    
                    if message[1] == last_message - 1:
                        for issue in message[0]:
                            cur = cnx.cursor()
                            cur.execute("SELECT * FROM Githubevent WHERE issue_id = %s", (issue[0],))
                            item = cur.fetchone()
                            if item:
                                if item[8] == "CLOSED":
                                    cur.execute("UPDATE Githubevent set state %s where issue_id = %s", (issue[8], issue[0]))
                                    cur.execute("UPDATE Githubevent set updated_at %s where issue_id = %s", (issue[5], issue[0])) 
                                else: 
                                    cur.execute("UPDATE Githubevent set state %s where issue_id = %s", (issue[8], issue[0])) 
                                    new_elapsed = item[7] + (issue[5] - item[5])
                                    cur.execute("UPDATE Githubevent set elapsed %s where issue_id = %s", (new_elapsed, issue[0])) 
                                    cur.execute("UPDATE Githubevent set updated_at %s where issue_id = %s", (issue[5], issue[0])) 
                            else:
                                cur.execute("INSERT IGNORE INTO Githubevent VALUES %s", (issue,))
                            cursor.executemany(query, events)
                            cnx.commit()
                            cursor.close()
                        message += 1

                    message.complete()
                    if str(message) == 'shutdown':
                        session.set_session_state("CLOSED")
        except NoActiveSession:
            return


def sample_session_send_receive_with_pool(sb_config, queue):

    concurrent_receivers = 5
    sessions = [str(uuid.uuid4()) for i in range(2 * concurrent_receivers)]
    client = ServiceBusClient(
        service_namespace=sb_config['hostname'],
        shared_access_key_name=sb_config['key_name'],
        shared_access_key_value=sb_config['access_key'],
        debug=False)
    bus_service = client.get_queue(queue)

    # topic_options = Topic()
    # topic_options.max_size_in_megabytes = '5120'
    # topic_options.default_message_time_to_live = 'PT1M'
    # bus_service.create_topic('data_pipeline', topic_options)
    # bus_service.create_subscription('data_Pipeline', 'AllMessages')

    for session_id in sessions:
        with bus_service.get_sender(session=session_id) as sender:
            for i in range(1):
                # message = Message("Sample message no. {}".format(i))
                # sender.send(message)
                faster_download_upload_3(sender, i)
                
    all_messages = []
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_receivers) as thread_pool:
        for _ in range(concurrent_receivers):
            futures.append(thread_pool.submit(message_processing, bus_service, all_messages))
        concurrent.futures.wait(futures)

    print("Received total {} messages across {} sessions.".format(len(all_messages), 2*concurrent_receivers))

def data_summarizer():
    

# if __name__ == '__main__':

def main(msg: func.ServiceBusMessage):
    logging.info('Python ServiceBus topic trigger processed message: %s',
                 msg.get_body().decode('utf-8'))

    if msg.get_body == "Start":
        live_config = conftest.get_live_servicebus_config()
        queue_name = conftest.create_session_queue(live_config)
        print("Created queue {}".format(queue_name))
        try:
            sample_session_send_receive_with_pool(live_config, queue_name)
        finally:
            print("Cleaning up queue {}".format(queue_name))