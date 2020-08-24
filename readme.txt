## usage

simple rpc amqp python library on pike

### consumer

```python
s = ServiceCreator('amqps://user:password@host.cloudamqp.com/vhost',
                   "exchangepypy")


def cb(msg):
    print(msg)
    return msg


s.consume("py.test", "", cb)

```

### rpc request

```python
s = ServiceCreator('amqps://user:password@host.cloudamqp.com/vhost',
                   "exchangepypy")


res,err = s.rpc_request("py.test", "sdsdsdsd", 0)
print(res,err)


```

### fire and forget

```python
s = ServiceCreator('amqps://user:password@host.cloudamqp.com/vhost',
                   "exchangepypy")

s.fire_and_forget("py.test", "cvvccvvcv")


```