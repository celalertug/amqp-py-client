import uuid
import pika
import threading
import time


def set_timeout(sec, cb, *args, **kwargs):
    op = {"cancelled": False}

    def cancel():
        op["cancelled"] = True

    def wait():
        time.sleep(sec)
        if op["cancelled"] is False:
            cb(*args, **kwargs)

    threading.Thread(target=wait).start()
    return cancel


class ServiceCreator:
    def __init__(self, url, exchange):
        self.exchange = exchange
        self.params = pika.URLParameters(url)
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()

        self.consumer_threads = []

    def consume(self, topic, queue="", cb=None):
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='topic')

        result = self.channel.queue_declare(queue=queue, exclusive=False, auto_delete=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=topic)

        def callback(ch, method, properties, body):
            correlation_id = properties.correlation_id
            reply_to = properties.reply_to
            if callable(cb):
                ret = cb(body.decode())
                if isinstance(ret, str) and reply_to is not None and correlation_id is not None:
                    ch.basic_publish(exchange=self.exchange,
                                     routing_key=reply_to,
                                     properties=pika.BasicProperties(correlation_id=correlation_id),
                                     body=ret)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False)

        t = threading.Thread(target=self.channel.start_consuming)
        self.consumer_threads.append(dict(id=str(uuid.uuid4()), topic=topic, thread=t))
        t.start()
        # self.channel.start_consuming()

    def rpc_request(self, topic, msg, timeout_sec=0):
        tmp = {"reply_to": str(uuid.uuid4()), "correlation_id": str(uuid.uuid4()), "received": None, "error": None}

        connection = pika.BlockingConnection(self.params)
        channel = connection.channel()

        result = channel.queue_declare(queue="", exclusive=True, auto_delete=True, durable=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=tmp["reply_to"])

        def timeout_handler():
            print("timeout reached")
            tmp["error"] = "timeout"
            channel.basic_cancel(tmp["consumer_tag"])
            channel.close()
            connection.close()

        if timeout_sec > 0:
            print("set timeout")
            cancel_timeout = set_timeout(timeout_sec, timeout_handler)

        def callback(ch, method, properties, body):
            correlation_id = properties.correlation_id
            if correlation_id != tmp["correlation_id"]:
                return

            tmp["received"] = body.decode()
            cancel_timeout()
            channel.basic_cancel(tmp["consumer_tag"])
            channel.close()
            connection.close()

        tmp["consumer_tag"] = channel.basic_consume(
            queue=queue_name, on_message_callback=callback, exclusive=True, auto_ack=True)

        channel.basic_publish(exchange=self.exchange,
                              routing_key=topic,
                              properties=pika.BasicProperties(reply_to=tmp["reply_to"],
                                                              correlation_id=tmp["correlation_id"]),
                              body=msg)

        try:
            channel.start_consuming()
        except:
            pass
        return tmp["received"], tmp["error"]

    def fire_and_forget(self, topic, msg):
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=topic,
                                   body=msg)

    def close(self):
        self.channel.close()
        self.connection.close()
