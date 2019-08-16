
# import pytest
import os
import sys
import uuid
# import pytest

# Ignore async tests for Python < 3.5
collect_ignore = []
if sys.version_info < (3, 5):
    collect_ignore.append("tepytest --versionsts/async_tests")
    collect_ignore.append("examples/async_examples")

def get_live_servicebus_config():
    config = {}
    config['hostname'] = 'jsoyservicebus'
    config['key_name'] = 'RootManageSharedAccessKey'
    config['access_key'] = 'JEG/7WF30Yur8UalzGCfwAXlW/t3Zpd95+5V2WP9C3Y='
    config['conn_str'] = 'Endpoint=sb://jsoyservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=JEG/7WF30Yur8UalzGCfwAXlW/t3Zpd95+5V2WP9C3Y='
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



def create_session_queue(servicebus_config, client=None):
    from azure.servicebus.control_client import ServiceBusService, Queue
    queue_name = str(uuid.uuid4())
    queue_value = Queue(
        lock_duration='PT30S',
        requires_duplicate_detection=False,
        requires_session=True)
    client = client or ServiceBusService(
        service_namespace=servicebus_config['hostname'],
        shared_access_key_name=servicebus_config['key_name'],
        shared_access_key_value=servicebus_config['access_key'])
    if client.create_queue(queue_name, queue=queue_value, fail_on_exist=True):
        return queue_name
    raise ValueError("Queue creation failed.")


