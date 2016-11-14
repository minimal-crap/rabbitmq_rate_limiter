"""
This module defines classes and methods to perform message publishing on specified
queue belonging to specific rabbitmq exchange
"""
import uuid
import pika
from config import settings


class Publisher:
    """
    Publisher class, defines methods to publish message to specific queues.
    """
    def __init__(self,
                 exchange_name=None,
                 queue_name=None):
        """
        This method instantiates the Publisher object.

        :param exchange_name: str object; name of the exchange
        :param queue_name: str object; name of the queue
        """

        try:
            # getting channel name
            if exchange_name is not None:
                self.exchange_name = exchange_name
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
        """
        This method pushes number of messages in the target queue, specified by the count
        parameter.

        :param count: int object; numbes of messages to push
        :return: None
        """
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
