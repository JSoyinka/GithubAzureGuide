from azure.servicebus.control_client import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME

from azure.eventhub import EventHubClient, Sender, EventData, EventHubError
import logging

eventhubs = [
    EventHubClient(
        "sb://<namespace>.servicebus.windows.net/<ehname>",
        username="<keyname>",
        password="<key>",
    )
]

token = "<gh_token>"
ENDPOINT = "https://api.github.com/events?per_page=100"



bus_service = ServiceBusService(
    service_namespace='mynamespace',
    shared_access_key_name='sharedaccesskeyname',
    shared_access_key_value='sharedaccesskey')


bus_service.create_topic('data_pipeline')

topic_options = Topic()
topic_options.max_size_in_megabytes = '5120'
topic_options.default_message_time_to_live = 'PT1M'

bus_service.create_topic('data_pipeline', topic_options)

bus_service.create_subscription('mytopic', 'AllMessages')

bus_service.create_subscription('mytopic', 'HighMessages')

rule = Rule()
rule.filter_type = 'SqlFilter'
rule.filter_expression = 'messagenumber > 3'

bus_service.create_rule('mytopic', 'HighMessages', 'HighMessageFilter', rule)
bus_service.delete_rule('mytopic', 'HighMessages', DEFAULT_RULE_NAME)

for eh_client in eventhubs:
    senders.append(
        EventbusSender(
            eh_client.add_sender(),
            flush_cb=lambda took, count: debug(
                "EVENTHUB REQUEST | took: {} sec, sent {} records to {}.".format(took, count, eh_client.eh_name)
            ),
        )
    )
    failed = eh_client.run()
    if failed:
        raise EventHubError("Couldn't connect to EH {}".format(eh_client.eh_name))


bus_service.create_subscription('mytopic', 'LowMessages')

rule = Rule()
rule.filter_type = 'SqlFilter'
rule.filter_expression = 'messagenumber <= 3'

bus_service.create_rule('mytopic', 'LowMessages', 'LowMessageFilter', rule)
bus_service.delete_rule('mytopic', 'LowMessages', DEFAULT_RULE_NAME)

for i in range(5):
    msg = Message('Msg {0}'.format(i).encode('utf-8'), custom_properties={'messagenumber':i})
    bus_service.send_topic_message('mytopic', msg)


# Receiving messages
msg = bus_service.receive_subscription_message('mytopic', 'LowMessages', peek_lock=False)
print(msg.body)


msg = bus_service.receive_subscription_message('mytopic', 'LowMessages', peek_lock=True)
if msg.body is not None:
print(msg.body)
msg.delete()
