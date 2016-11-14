"""
This module implements the rate limiter class and create a rabbitmq connection
to be used by it.
queue and exchange name provided in cli args should be already created and bound.

Example:
    python main.py --queue-name sample_queue --exchange_name sample_exchange
    python main.py -q test_queue -e test_exchange

"""
import threading

import argparse
import pika

from limiter import RateLimiter
from config import settings

# implementing command line parser for provided arguments
parser_obj = argparse.ArgumentParser(description="main module argument parser")
parser_obj.add_argument('--queue-name',
                        '-q',
                        dest="queue_name",
                        action="store",
                        type=str,
                        default=None)
parser_obj.add_argument('--exchange-name',
                        '-e',
                        dest="exchange_name",
                        action="store",
                        type=str,
                        default=None)
parsed_args = parser_obj.parse_args()
queue_name = parsed_args.queue_name
exchange_name = parsed_args.exchange_name

# creating connection to the rabbitmq server
conn = pika.BlockingConnection(pika.ConnectionParameters(**settings.RABBITMQ_CONNECTION_PARAMETER))


def build_limiter(queue_name=None,
                  exchange_name=None):
    """
    This method takes queue_name & exchange_name as parameter and instantiate
    the rate limiter over the source queue belonging to corresponding exchange

    :param queue_name: string object
    :param exchange_name: string object
    :return: None

    Example:
    build_limiter("sample_queue", "sample_exchange") # for sample_queue & sample_exchange
    build_limiter() # to take default queue and exchange_name
    """

    if queue_name is None:
        queue_name = "sample_queue"
    if exchange_name is None:
        exchange_name = "sample_exchange"

    # apply the message_threshold and the time_cycle
    # as per the requirement of the application
    rl = RateLimiter(message_threshold=4,
                     time_cycle=1.0,
                     source_queue=queue_name,
                     exchange=exchange_name,
                     connection=conn)
    source_queue_count = rl.check_source_queue_count()
    filter_queue_count = rl.check_filter_queue_count()
    print source_queue_count
    print filter_queue_count
    if source_queue_count > 0 and filter_queue_count == 0:
        current_count = rl.message_threshold if source_queue_count > rl.message_threshold else source_queue_count
        print current_count
        message_list = rl.get_from_source_queue(current_count)
        rl.push_to_filter_queue(message_list=message_list)
    threading.Timer(rl.time_cycle, build_limiter).start()


if __name__ == '__main__':
    build_limiter(queue_name, exchange_name)
