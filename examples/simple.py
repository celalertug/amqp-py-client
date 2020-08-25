from amqp_client import ServiceCreator

s = ServiceCreator('amqp://localhost',
                   "exchangepypy")


def cb(msg):
    print("cb", msg)
    return msg


def cb2(msg):
    print("cb2", msg)
    return msg


def cb3(msg):
    print("cb3", msg)
    return msg


s.consume("py.test", "", cb)
s.consume("py.test2", "", cb2)
s.consume("py.test3", "", cb3)

res = s.rpc_request("py.test", "12312312")
print(res)
res = s.rpc_request("py.test2", "dfdfdf")
print(res)

s.fire_and_forget("py.test3", "vbvbvbc")
