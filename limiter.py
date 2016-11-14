import pika


class RateLimiter:
    def __init__(self,
                 message_threshold=4,
                 time_cycle=1.0,
                 source_queue=None,
                 exchange=None,
                 connection=None):


        self.message_threshold = message_threshold
        self.time_cycle = time_cycle
        self.exchange = exchange
        if source_queue is not None and connection is not None:
            self.channel = connection.channel()
            self.filter_queue = self.channel.queue_declare(queue=source_queue + "_fl", durable=True)
            self.source_queue = self.channel.queue_declare(queue=source_queue, durable=True)
            self.channel.queue_bind(self.source_queue.method.queue, self.exchange)
            self.channel.queue_bind(self.filter_queue.method.queue, self.exchange)

    def check_source_queue_count(self):
        return self.source_queue.method.message_count

    def check_filter_queue_count(self):
        return self.filter_queue.method.message_count

    def push_to_filter_queue(self, message_list=None):
        if message_list is not None and self.check_filter_queue_count() == 0:
            for message in message_list:
                self.channel.basic_publish(exchange=self.exchange, routing_key=self.filter_queue.method.queue,
                                           body=message,
                                           properties=pika.BasicProperties(content_type="text/plain"))

    def get_from_source_queue(self, count=None):
        message_list = list()
        if self.check_source_queue_count != 0 and count is not None:
            for i in range(count):
                method_frame, header_frame, body = self.channel.basic_get(queue=self.source_queue.method.queue)
                message_list.append(body)
                if method_frame.NAME == "Basic.GetEmpty":
                    break
                else:
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return message_list






