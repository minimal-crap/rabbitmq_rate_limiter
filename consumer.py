"""
This module defines classes and methods to perform consumption of any specific queue
belonging to a specific rabbitmq exchange.
"""
import pika

from config import settings


class Consumer:
    """
    Consumer class to consume the data from the source queue whichever identified.
    """

    def __init__(self,
                 queue_name=None,
                 exchange_name=None):
        """
        This method instantiates the Consumer object.

        :param queue_name: str object; name of queue
        :param exchange_name:  str object; name of exchange
        """
        # declaring rabbitmq connection
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(**settings.RABBITMQ_CONNECTION_PARAMETER))
        self.channel = self.connection.channel()
        # getting the exchange name value
        if exchange_name is not None:
            self.exchange_name = exchange_name
        else:
            self.exchange_name = "sample_exchange"

        # getting the queue name
        if queue_name is not None:
            self.queue_name = queue_name
        else:
            self.queue_name = "sample_queue"
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      type="direct")
        self.channel.queue_declare(queue=self.queue_name,
                                   durable=True)

        # binding declared queue and channel
        self.channel.queue_bind(self.queue_name,
                                self.exchange_name)

    def callback(self, ch, method, properties, body):
        """
        This method acts as callback for rabbitmq Consumer / Worker.
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return: None
        """
        print("[*]message received: {}".format(body))
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """
        This method binds the callback method with rabbitmq channel and initiates the message
        consumption.
        :return: None
        """
        self.channel.basic_consume(self.callback,
                                   self.queue_name)
        self.channel.start_consuming()
