# _*_ coding: utf-8 _*_
"""
This module defines classes and methods necessary to perform message
publishing, message consuming and rate limiting of message consumption.
"""

import uuid
import pika
import settings


class Publisher:
    """
    Publisher class, defines methods to publish message to specific queues.
    Args:
        exchange_name (str): rabbitmq exchage name.
        queue_name (str): rabbitmq queue name.
    """

    def __init__(self,
                 exchange_name=None,
                 queue_name=None):
        try:
            if exchange_name is None or queue_name is None:
                raise ValueError
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    **settings.RABBITMQ_CONNECTION_PARAMETER))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name)
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.queue_bind(queue_name, exchange_name)
            self.queue_name = queue_name
            self.exchange_name = exchange_name

        except ValueError as error:
            print('Publisher::__init__:{}'.format(error.message))

        except Exception as error:
            print('Publisher::__init__:{}'.format(error.message))

    def publish_message(self):
        """
        publish_message method, pushes a random uuid to the
        queue defined while instantiating Publisher class.
        """
        try:
            message = "call_uuid:{}".format(str(uuid.uuid4()))
            print("[*]:{} => queue::{}".format(
                message,
                self.queue_name))

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(content_type="text/plain"))
        except Exception as error:
            print("Publisher::publish_messages:{}".format(error.message))


class RateLimiter:
    """
    RateLimiter class, defines method to limit consumption of
    specified queue messages.

    Args:
        message_threshold (int): consumption rate of messages per second.
        time_cycle (int): frequency in seconds for consumer thread.
        source_queue (str): source queue name to limit.
        exchange_name (str): exchange name of source queue.
        connection (pika.adapters.blocking_connection.BlockingConnection): pika
        connection object of rabbitmq server.
    """

    def __init__(self,
                 message_threshold=settings.MESSAGE_THRESHOLD,
                 time_cycle=settings.CONSUMER_FREQUENCY,
                 source_queue=None,
                 exchange=None,
                 connection=None):
        self.message_threshold = message_threshold
        self.time_cycle = time_cycle
        self.exchange = exchange
        self.queue_types = ['source', 'filter']
        try:
            if source_queue is None or exchange is None:
                raise ValueError
            self.channel = connection.channel()

            # creating filter queue reference.
            self.filter_queue = self.channel.queue_declare(
                queue=source_queue + "_filter",
                durable=True)

            # creating source queue reference.
            self.source_queue = self.channel.queue_declare(
                queue=source_queue,
                durable=True)

            # binding both queues with same exchange.
            self.channel.queue_bind(
                self.source_queue.method.queue,
                self.exchange)
            self.channel.queue_bind(
                self.filter_queue.method.queue,
                self.exchange)

        except ValueError as error:
            print('RateLimiter::__init__:{}'.format(error.message))
        except Exception as error:
            print('RateLimiter::__init__:{}'.format(error.message))

    def get_queue_count(self, queue_type=None):
        """returns source queue messages count.
        Returns:
            int: source queue messages count.
        """
        try:
            if queue_type not in self.queue_types:
                raise ValueError

            if queue_type == 'source':
                return self.source_queue.method.message_count
            else:
                return self.filter_queue.method.message_count
        except ValueError as error:
            print('RateLimiter::get_queue_count:{}'.format(error.message))

    def push_to_filter_queue(self, message_list=None):
        """pushes messages list to the filter queue.

        Args:
            message_list (List): list of messages.

        Returns:
            boolean: True if success, false otherwise.

        Raises:
            ValueError: if `message_list` type is not list.
        """
        try:
            if not type(message_list) == list:
                raise ValueError
            if self.get_queue_count(queue_type='filter') == 0:
                for message in message_list:
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.filter_queue.method.queue,
                        body=message,
                        properties=pika.BasicProperties(
                            content_type="text/plain"))
            return True
        except ValueError as error:
            print('RateLimiter::push_to_filter_queue:{}'.format(error.message))
            return False

    def get_from_source_queue(self, count=None):
        """returns `n` messages from source defined by count param.

        Args:
            count (int): messages count to fetch.

        Returns:
            list: messages list if success, None otherwise.

        Raises:
            ValueError: if count param is not type int
        """
        message_list = []
        try:
            if not type(count) == int:
                raise ValueError
            if self.get_queue_count(queue_type='source') != 0:
                for i in range(count):
                    method_frame, header_frame, body = self.channel.basic_get(
                        queue=self.source_queue.method.queue)
                        message_list.append(body)
                    if method_frame.NAME == "Basic.GetEmpty":
                        break
                    else:
                        self.channel.basic_ack(
                            delivery_tag=method_frame.delivery_tag)
            return message_list
        except ValueError as error:
            print('RateLimiter::get_from_source_queue:{}'.format(
                error.message))
            return None
        except Exception as error:
            print('RateLimiter::get_from_source_queue:{}'.format(
                error.message))
            return None


class Consumer:
    """Consumer class, consumes data from specified/source queue.

    Args:
        queue_name (str): queue name to consume from.
        exchange_name (str): exchange name containing queue.

    Raises:
        ValueError: if queue_name/exchange_name is/are not valid type.
    """

    def __init__(self,
                 queue_name=None,
                 exchange_name=None):

        try:
            if not type(queue_name) == str or not type(exchange_name) == str:
                raise ValueError
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    **settings.RABBITMQ_CONNECTION_PARAMETER))
            self.channel = self.connection.channel()

            # declare and bind exchange with queue
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                type="direct")
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True)
            self.channel.queue_bind(
                self.queue_name,
                self.exchange_name)

        except ValueError as error:
            print('Consumer::__init__:{}'.format(error.message))
        except Exception as error:
            print('Consumer::__init__:{}'.format(error.message))

    def callback(self, channel, method, properties, body):
        """pika consumer callback method.

        Args:
            channel (pika.channel.Channel): rabbitmq channel object.
            method (pika.spec.Basic.Deliver): pika internal class.
            properties (pika.spec.BasicProperties): pika internal class.
            body (str, unicodes, or bytes): message body.
        """
        print("[*]message received: {}".format(body))
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """ binds rabbitmq channel and callback, and starts consumption.
        """
        self.channel.basic_consume(self.callback,
                                   self.queue_name)
        self.channel.start_consuming()
