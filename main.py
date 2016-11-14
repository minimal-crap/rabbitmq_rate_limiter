import threading

import pika

from limiter import RateLimiter
from config import settings

conn = pika.BlockingConnection(pika.ConnectionParameters(**settings.RABBITMQ_CONNECTION_PARAMETER))


def build_limiter():
    # apply the message_threshold and the time_cycle
    # as per the requirement of the application
    rl = RateLimiter(message_threshold=4, time_cycle=1.0, source_queue="sample_queue", exchange="direct_logs",
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


build_limiter()
