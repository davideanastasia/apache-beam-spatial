import time
import sys
import os

from datetime import datetime

from google.cloud import pubsub_v1

project_id = os.getenv('GCP_PROJECT')
topic_name = 't-drive-data'

DT_FMT = '%Y-%m-%d %H:%M:%S'
TS_ATTR_FMT = "%Y-%m-%dT%H:%M:%S.000Z"
in_flight_messages = dict()


def get_callback(_f, _msg_id):
    def callback(_f):
        try:
            print(_msg_id, _f.result())
            in_flight_messages.pop(_msg_id)
        except:  # noqa
            print('Please handle {} for {}.'.format(_f.exception(), _msg_id))

    return callback


def execute():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    base_time = datetime.utcnow().replace(microsecond=0)
    base_time_delta = None
    row = None
    epoch = 0

    do_run = True
    while do_run:
        with open(sys.argv[1], 'r') as input_data:
            for next_row_num, next_row in enumerate(input_data):
                next_row_dt = datetime.strptime(next_row.strip().split(',')[1], DT_FMT)

                if row is None:
                    # I am the first row...
                    base_time_delta = base_time - next_row_dt
                else:
                    row_data = row.strip()
                    row_tokens = row_data.split(',')
                    row_dt = datetime.strptime(row_tokens[1], DT_FMT)
                    # add adjusted timestamp to final message
                    row_tokens[1] = (row_dt + base_time_delta).strftime(DT_FMT)

                    msg_id = '{}:{}'.format(epoch, next_row_num - 1)
                    msg_data = ','.join(row_tokens).encode('utf-8')     # data must be a bytestring.

                    in_flight_messages.update({msg_id: None})

                    # When you publish a message, the client returns a future.
                    ts_attr = (row_dt + base_time_delta).strftime(TS_ATTR_FMT)
                    msg_future = publisher.publish(topic_path, data=msg_data, ts_attr_key=ts_attr)
                    in_flight_messages[msg_id] = msg_future

                    # Publish failures shall be handled in the callback function.
                    msg_future.add_done_callback(get_callback(msg_future, msg_id))

                    wait_time_secs = (next_row_dt - row_dt).total_seconds()
                    # print(row_tokens, [row_dt, base_time, base_time_delta, wait_time_secs, ts_attr])
                    if wait_time_secs > 0:
                        time.sleep(wait_time_secs)

                row = next_row

        epoch += 1


execute()