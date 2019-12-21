# coding: utf8 
import time
import json
from publisher import Publisher
from consumer import Consumer


host = 'amqp://guest:guest@120.27.154.15:5672/%2F'
exchange = 'test'
queue = 'test'


def test_publisher():
    p = Publisher(host=host, exchange=exchange, queue=queue)
    for i in range(5):
        p.publish(json.dumps({'foo': 'bar'}))
    print('queue size is {}'.format(p.qsize()))


def test_consumer():
    class Worker(Consumer):
        def consume(self, body):
            d = json.loads(body)
            self.log.info('foo is {}'.format(d['foo']))

    w = Worker(host=host, exchange=exchange, queue=queue)
    w.daemon = True
    w.start()

    print('wait 4s..')
    time.sleep(4)
    print('exit')


test_publisher()
test_consumer()
