# coding: utf8
import time
import pika
from pybase import BaseObject


class Publisher(BaseObject):
    def __init__(self, host, queue, exchange, exchange_type='topic', 
            routing_key=None):
        BaseObject.__init__(self)
        self.host = host
        self.queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        if not self.routing_key:
            self.routing_key = self.queue + '.all'
        self._connect = None
        self._channel = None
        self._queue = None
        self.connect()


    def connect(self):
        try:
            self._connect = pika.BlockingConnection(
                    pika.URLParameters(self.host))
            self._channel = self._connect.channel()
            self._channel.exchange_declare(exchange=self.exchange, 
                    exchange_type=self.exchange_type)
            self._queue = self._channel.queue_declare(queue=self.queue, 
                    durable=True)
            self._channel.queue_bind(exchange=self.exchange, 
                    queue=self.queue, routing_key=self.routing_key)
            self._channel.basic_qos(prefetch_count=1)
        except Exception as e:
            self.log.exception(e)
            time.sleep(1)


    def do_publish(self, message):
        self._channel.basic_publish(exchange=self.exchange, 
                routing_key=self.routing_key, body=message)


    def publish(self, message, retry=1):
        while retry > 0:
            try:
                self.do_publish(message)
                return True
            except Exception as e:
                self.log.exception(e)
                self.connect()
            retry -= 1
        self.log.error('publish message failed: [%s]' % message)
        return False

    
    def qsize(self):
        try:
            return self._queue.method.message_count
        except Exception as e:
            self.log.exception(e)
            return 0 

