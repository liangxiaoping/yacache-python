#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Author: liangxiaoping
#

from yacache import get_client

conf = {
    'url': 'redis://127.0.0.1:6379/0',
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'expiration_time': 60*60*2,   # 2 hours
    'distributed_lock': True
}

client = get_client(url='redis://127.0.0.1:6379/0')
#client = get_client(**conf)

# set
client.set('test_key1', 'test_value1')
client.set('test_key2', 'test_value2')

# get
print client.get('test_key1')
print client.get('test_key2')

# set_multi
data1 = dict(test_key3='test_value3')
client.set_multi(data1)
data2 = dict(test_key4='test_value4', test_key5='test_value5')
client.set_multi(data2)

# get_multi
print client.get_multi(data1)
print client.get_multi(data2)
print client.get('test_key3')
print client.get('test_key4')
print client.get('test_key5')

# delete
client.delete('test_key1')
print client.get('test_key1')

client.delete('test_key2')
print client.get('test_key2')

# delete_multi
client.delete_multi(('test_key3',))
client.delete_multi(('test_key4', 'test_key5'))
print client.get_multi(data1)
print client.get_multi(data2)