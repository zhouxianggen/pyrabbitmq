# coding: utf8
import time
import pika
from pyobject import PyObject


class Publisher(PyObject):
    def __init__(self, url, exchange, queue, exchange_type='topic', 
            routing_key=None):
        PyObject.__init__(self)
        self.url = url
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key
        if not self.routing_key:
            self.routing_key = self.queue + '.all'
        self._connect = None
        self._channel = None
        self.connect()

    def connect(self):
        try:
            self._connect = pika.BlockingConnection(
                    pika.URLParameters(self.url))
            self._channel = self._connect.channel()
            self._channel.exchange_declare(exchange=self.exchange, 
                    exchange_type=self.exchange_type)
            self._channel.queue_declare(queue=self.queue, durable=True)
            self._channel.queue_bind(exchange=self.exchange, 
                    queue=self.queue, routing_key=self.routing_key)
            self._channel.basic_qos(prefetch_count=1)
        except Exception as e:
            self.log.exception(e)
            time.sleep(1)

    def _publish(self, message):
        self._channel.basic_publish(exchange=self.exchange, 
                routing_key=self.routing_key, body=message)

    def publish(self, message, retry=1):
        while retry > 0:
            try:
                self.log.info('publish message: %s' % (message))
                self._publish(message)
                return True
            except Exception as e:
                self.log.exception(e)
                self.connect()
            retry -= 1
        self.log.error('publish message failed: [%s]' % message)
        return False

