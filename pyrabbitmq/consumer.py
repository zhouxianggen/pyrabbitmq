# coding: utf8
import time
import pika
from threading import Thread
from pyobject import PyObject


class Consumer(Thread, PyObject):
    def __init__(self, url, exchange, queue, exchange_type='topic', 
            routing_key=None):
        Thread.__init__(self)
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
        self._close = False
        self._tag = None

    def connect(self):
        while True:
            try:
                self.log.info('connecting to %s' % self.url)
                return pika.SelectConnection(pika.URLParameters(self.url),
                                             self.on_connection_open)
            except Exception as e:
                self.log.exception(e)
                time.sleep(1)

    def on_connection_open(self, unused_connect):
        self.log.info('connection opened')
        self._connect.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._close:
            self._connect.ioloop.stop()
        else:
            self.log.warning('connection closed, reopening in 5s: (%s) %s' % (
                            reply_code, reply_text))
            self._connect.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connect.ioloop.stop()
        if not self._close:
            # Create a new connection
            self._connect = self.connect()
            # There is now a new connection, needs a new ioloop to run
            self._connect.ioloop.start()

    def open_channel(self):
        self.log.info('creating a new channel')
        self._connect.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self.log.info('channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self.exchange)

    def on_channel_closed(self, *args, **kwargs):
        self.log.warning('channel was closed')
        self._connect.close()

    def setup_exchange(self, exchange_name):
        self.log.info('declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type)

    def on_exchange_declareok(self, unused_frame):
        self.log.info('exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        self.log.info('declaring queue %s' % queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, 
                durable=True)

    def on_queue_declareok(self, method_frame):
        self.log.info('binding %s to %s with %s' % (
                self.exchange, self.queue, self.routing_key))
        self._channel.queue_bind(self.on_bindok, self.queue,
                                 self.exchange, self.routing_key)
        self._channel.basic_qos(prefetch_count=1)

    def on_bindok(self, unused_frame):
        self.log.info('queue bound')
        self.start_consuming()

    def start_consuming(self):
        self.log.info('issuing consumer related RPC commands')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._tag = self._channel.basic_consume(
                self.on_message, self.queue)
    
    def on_consumer_cancelled(self, method_frame):
        self.log.info('consumer was cancelled remotely, shutting down: %r' % 
                method_frame)
        if self._channel:
            self._channel.close()

    def consume(self, body):
        pass

    def on_message(self, unused_channel, basic_deliver, properties, body):
        try:
            self.log.info('received message # %s from %s: %s' % (
                    basic_deliver.delivery_tag, properties.app_id, body))
            self.consume(body)
        except Exception as e:
            self.log.exception(str(e))
        self._channel.basic_ack(basic_deliver.delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self.log.info('sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._tag)

    def on_cancelok(self, unused_frame):
        self.log.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        self.log.info('closing the channel')
        self._channel.close()

    def run(self):
        self._connect = self.connect()
        self._connect.ioloop.start()

    def stop(self):
        self.log.info('stopping')
        self._close = True
        self.stop_consuming()
        self._connect.ioloop.start()
        self.log.info('stopped')

    def close_connect(self):
        self.log.info('closing connection')
        self._connect.close()

