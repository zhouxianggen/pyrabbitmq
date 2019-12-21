pyrabbitmq
![](https://img.shields.io/badge/python%20-%203.8-brightgreen.svg)
========
> rabbitmq consumer, publisher

## `Install`
` pip install git+https://github.com/zhouxianggen/pyrabbitmq.git`

## `Upgrade`
` pip install -U git+https://github.com/zhouxianggen/pyrabbitmq.git`

## `Uninstall`
` pip uninstall pyrabbitmq`

## `Basic Usage`
```python
from pyrabbitmq import Publisher, Consumer

host = 'amqp://guest:guest@localhost:5672/%2F'
exchange = 'test'
queue = 'test'

# Publisher
p = Publisher(host=host, exchange=exchange, queue=queue)
p.publish(json.dumps({'foo': 'bar'}))

# Consumer
class Worker(Consumer):
    def consume(self, body):
        self.log.info('message is {}'.format(body))

w = Worker(host=host, exchange=exchange, queue=queue)
w.daemon = True
w.start()
w.join()

```
