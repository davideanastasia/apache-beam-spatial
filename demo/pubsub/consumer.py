import time
import json
import requests
import os
import signal
import logging

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
do_run = True


def signal_handler(sig, frame):
    global do_run
    print('You pressed Ctrl+C!')
    do_run = False


taxis = set()
MAX_TAXI_COUNT = 300


def callback(message):
    try:
        decoded_json = json.loads(message.data)
        if decoded_json['timing'] == 'ON_TIME':
            r = requests.post('http://127.0.0.1:5000/remove', json=decoded_json)
            logger.info('remove: %s, %s', decoded_json['id'], r.status_code)
        elif decoded_json['timing'] == "EARLY" and (len(taxis) < MAX_TAXI_COUNT or decoded_json['id'] in taxis):
            r = requests.post('http://127.0.0.1:5000/insert', json=decoded_json)
            taxis.add(decoded_json['id'])
            logger.info('insert: %s, %s', decoded_json['id'], r.status_code)
    except Exception as e:
        logger.error(e)

    message.ack()


signal.signal(signal.SIGINT, signal_handler)

project_id = os.getenv('GCP_PROJECT')
topic_name = 't-drive-session'
topic_path = 'projects/{}/topics/{}'.format(project_id, topic_name)
subscription_name = topic_name + '.subscription-' + str(int(time.time() * 1000))

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)
create_subscription = subscriber.create_subscription(subscription_path, topic_path)

pull_subscription = subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background
logger.info('Listening for messages on %s', subscription_path)
while do_run:
    time.sleep(5)

logger.info('main loop completed')
pull_subscription.cancel()
subscriber.delete_subscription(subscription_path)
logger.info('done')
