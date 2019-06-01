# coding: utf8 
import time
import json
from publisher import Publisher
from consumer import Consumer
from utils import qsize

#url = 'amqp://guest:guest@106.14.113.231:5672/%2F'
url = 'amqp://guest:guest@114.55.176.222:5672/%2F'
exchange = 'test'
queue = 'test'


def test_publisher():
    p = Publisher(url=url, exchange=exchange, queue=queue)
    for i in range(5):
        p.publish(json.dumps({'foo': 'bar'}))
    print('queue size is {}'.format(
            qsize(url=url, exchange=exchange, queue=queue)))


def test_consumer():
    class Worker(Consumer):
        def consume(self, body):
            d = json.loads(body)
            self.log.info('foo is {}'.format(d['foo']))

    w = Worker(url=url, exchange=exchange, queue=queue)
    w.daemon = True
    w.start()

    print('wait 4s..')
    time.sleep(4)
    print('exit')


#test_publisher()
test_consumer()
