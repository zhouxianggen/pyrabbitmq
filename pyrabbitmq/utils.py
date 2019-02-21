# coding: utf8
import pika


def qsize(url, exchange, queue, exchange_type='topic'):
    try:
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        q = channel.queue_declare(queue=queue, durable=True)
        return q.method.message_count
    except Exception as e:
        return 0 

