pyrabbitmq
![](https://img.shields.io/badge/python%20-%203.7-brightgreen.svg)
========
> provide python rabbitmq consumer, publisher

## `Install`
` pip install git+https://github.com/zhouxianggen/pyrabbitmq.git`

## `Upgrade`
` pip install --upgrade git+https://github.com/zhouxianggen/pyrabbitmq.git`

## `Uninstall`
` pip uninstall pyrabbitmq`

## `Basic Usage`
```python
from pyrabbitmq import Publisher, Consumer

url = 'amqp://guest:guest@localhost:5672/%2F'
exchange = 'test'
queue = 'test'

# 发送消息
p = Publisher(url=url, exchange=exchange, queue=queue)
p.publish(json.dumps({'foo': 'bar'}))

# 消费消息
class Worker(Consumer):
    def consume(self, body):
        self.log.info('message is {}'.format(body))

w = Worker(url=url, exchange=exchange, queue=queue)
w.daemon = True
w.start()

```
