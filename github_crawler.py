from pprint import pprint
import requests
import json
import os
import sys
import time
import datetime
import traceback
from azure.eventhub import EventHubClient, Sender, EventData, EventHubError
import logging

eventhubs = [
    EventHubClient(
        "sb://jsoytesteventhub.servicebus.windows.net/testeventhub",
        username="RootManageSharedAccessKey",
        password="e/UyKgsa0nA+fHzW7OkXDDN1ze8Mo7dns86myVDVEWg=",
    )
]

token = "2f09b079648348219fbef824a411f1070de6eed2 "
ENDPOINT = "https://api.github.com/events?per_page=100"


class Tracker:
    interval = 60

    def __init__(self, tick_reports=None):
        self.track = time.time()
        self.sent_events = 0
        self.issued_requests = 0
        self.tick_reports = tick_reports or print

    def call(self):
        msg = "Tracker: events sent : {} calls made :{}".format(self.sent_events, self.issued_requests)
        self.tick_reports(msg)

    def report(self):
        now = time.time()
        if now - self.track >= 60:
            self.call()
            self.reset()
    
    def reset(self):
        self.track = time.time()
        self.sent_events = 0
        self.issued_requests = 0




class CacheSlide:
    def __init__(self, max_size=500):
        self.prev = set()
        self.current = set()
        self.max_size = max_size

    def add(self, item):
        if item not in self:
            if len(self.current) > self.max_size:
                self.prev = self.current
                self.current = set()

            self.current.add(item)

    def __contains__(self, item):
        return item in self.current or item in self.prev


class EventHubBufferControl:
    def __init__(self, sender, tick_flush, serializer=json.dumps, item_seperator="\n", max_size=250000):
        self.max_size = max_size
        self.buffer = ""
        self.item_count = 0
        self.sender = sender
        self.tick_flush = tick_flush
        self.item_seperator = item_seperator
        self.serializer = serializer

    def push(self, item):
        _item = "{}{}".format(self.serializer(item), self.item_seperator)
        if len(_item.encode("utf-8")) > self.max_size:
            raise EventHubError(
                "Item {} is to big ({}) where as limit is {}. Ignoring.".format(
                    _item, len(_item.encode("utf-8")), self.max_size
                )
            )

        if len((self.buffer + _item).encode("utf-8")) > self.max_size:
            self.flush()

        self.buffer += _item
        self.item_count += 1

    def flush(self):
        try:
            start = time.time()
            self.sender.send(EventData(self.buffer))
            self.tick_flush(time.time() - start, self.item_count)
        except Exception as e:
            raise EventHubError("lost the following {} records:\n {}".format(self.item_count, self.buffer))

        self.buffer = ""
        self.item_count = 0


def logger(m):
    print("{} | {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], m))


def debugger(message):
    # logger("{} | {}".format("DEBUG", message))
    pass


def info(message):
    logger("{} | {}".format("INFO", message))


def error(message, err):
    logger("{} | {} | {}".format("ERROR", message, err))

def run():
    info("STARTED github to eventhub")

    headers = {"Authorization": "token {}".format(token)}

    tracker = Tracker(info)

    senders = []

    for eventhub_client in eventhubs:
        senders.append(
            EventHubBufferControl(
                eventhub_client.add_sender(),
                tick_flush=lambda took, count: debugger(
                    "EVENTHUB REQUEST | took: {} sec, sent {} records to {}.".format(took, count, eventhub_client.eventhub_name)
                ),
            )
        )
        failed = eventhub_client.run()
        if failed:
            raise EventHubError("Couldn't connect to EH {}".format(eventhub_client.eventhub_name))

    seconds_per_request = round(
        1.0 / (5000 / 60 / 60), 2
    )  # requests / minutes / seconds = requests per sec, ^-1=secs per request

    cache = CacheSlide()

    events = 0

    loop = True
    while loop:
        loop_start_time = time.time()

        tracker.report()

        try:
            resp = requests.get(ENDPOINT, headers=headers)
            resp.raise_for_status()

            tracker.issued_requests += 1

            data = sorted(resp.json(), key=lambda x: x["id"])
            payload = ""

            debugger("GITHUB REQUEST | took {} sec, got {} events.".format(resp.elapsed.total_seconds(), len(data)))

            for d in data:
                if d["id"] not in cache:
                    for sent_buffer in senders:
                        try:
                            sent_buffer.push(d)
                        except EventHubError as e:
                            error("EventHubError", e.message)

                    tracker.sent_events += 1
                    cache.add(d.get("id"))

            cycle_took = time.time() - loop_start_time
            delay = seconds_per_request - cycle_took
            debugger("CYCLE DONE | took {}, waiting for {}".format(cycle_took, max(delay, 0)))
            if delay > 0:
                time.sleep(delay)

        except requests.HTTPError as e:
            if resp.status_code in [429, 403]:
                time_to_wait = int(
                    float(resp.headers.get("Reset for limit", 60)) - datetime.datetime.utcnow().timestamp()
                )
                info("waiting for {}".format(time_to_wait))
                if time_to_wait > 0:
                    time.sleep(time_to_wait)
            error("HTTP EXCEPTION", repr(e))
        except EventHubError as e:
            error("Failed to send events to eventhub, skipping", repr(e))
        except Exception as e:
            error("UNEXPECTED ERROR", repr(e))
            traceback.print_exc()

    os.kill(os.getpid(), 9)


if __name__ == "__main__":
    run()