import uuid
import pika
from config import settings


class Publisher:
    def __init__(self,
                 channel_name=None,
                 queue_name=None):

        try:
            # getting channel name
            if channel_name is not None:
                self.exchange_name = channel_name
            else:
                self.exchange_name = "sample_exchange"

            # getting queue_name
            if queue_name is not None:
                self.queue_name = queue_name
            else:
                self.queue_name = "sample_queue"

            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(**settings.RABBITMQ_CONNECTION_PARAMETER))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.queue_bind(self.queue_name, self.exchange_name)

        except Exception as err:
            print "Publisher::__init__ : {}".format(err.message)

    def publish_messages(self, count):
        try:
            for i in range(count):
                call_uuid = str(uuid.uuid4())
                message = "call_uuid:{}".format(call_uuid)
                print("[*]pushing call_uuid:{} to {}!".format(call_uuid,
                                                              self.queue_name))
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.queue_name, body=message,
                                           properties=pika.BasicProperties(content_type="text/plain"))
        except Exception as err:
            print "Publisher::publish_messages : {}".format(err.message)
