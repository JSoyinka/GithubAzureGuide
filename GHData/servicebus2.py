import uuid
import concurrent

import conftest

from azure.servicebus import ServiceBusClient, Message
from azure.servicebus.control_client import ServiceBusService, Topic, Rule, DEFAULT_RULE_NAME
from azure.servicebus.common.constants import NEXT_AVAILABLE
from azure.servicebus.common.errors import NoActiveSession
from faster_download_upload3 import faster_download_upload_3


def message_processing(queue_client, messages):
    while True:
        try:
            with queue_client.get_receiver(session=NEXT_AVAILABLE, idle_timeout=1) as session:
                session.set_session_state("OPEN")
                for message in session:
                    messages.append(message)
                    # print("Message: {}".format(message))
                    # print("Time to live: {}".format(message.header.time_to_live))
                    print("Sequence number: {}".format(message.sequence_number))
                    # print("Enqueue Sequence numger: {}".format(message.enqueue_sequence_number))
                    # print("Partition ID: {}".format(message.partition_id))
                    # print("Partition Key: {}".format(message.partition_key))
                    # print("Locked until: {}".format(message.locked_until))
                    # print("Lock Token: {}".format(message.lock_token))
                    # print("Enqueued time: {}".format(message.enqueued_time))
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
            for i in range(5):
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

def get_live_servicebus_config():
    config = {}
    config['hostname'] = os.environ['SERVICE_BUS_HOSTNAME']
    config['key_name'] = os.environ['SERVICE_BUS_SAS_POLICY']
    config['access_key'] = os.environ['SERVICE_BUS_SAS_KEY']
    config['conn_str'] = os.environ['SERVICE_BUS_CONNECTION_STR']
    return config


def create_standard_queue(servicebus_config, client=None):
    from azure.servicebus.control_client import ServiceBusService, Queue
    queue_name = str(uuid.uuid4())
    queue_value = Queue(
        lock_duration='PT30S',
        requires_duplicate_detection=False,
        dead_lettering_on_message_expiration=True,
        requires_session=False)
    client = client or ServiceBusService(
        service_namespace=servicebus_config['hostname'],
        shared_access_key_name=servicebus_config['key_name'],
        shared_access_key_value=servicebus_config['access_key'])
    if client.create_queue(queue_name, queue=queue_value, fail_on_exist=True):
        return queue_name
    raise ValueError("Queue creation failed.")

# if __name__ == '__main__':
live_config = conftest.get_live_servicebus_config()
queue_name = conftest.create_session_queue(live_config)
print("Created queue {}".format(queue_name))
try:
    sample_session_send_receive_with_pool(live_config, queue_name)
finally:
    print("Cleaning up queue {}".format(queue_name))