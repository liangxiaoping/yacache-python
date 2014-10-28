#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Author: liangxiaoping
#

import redis
from oslo.serialization import jsonutils


class RedisClient(object):
    """
    conf = {
        #'url' : 'redis://[:password]@localhost:6379/0',
        'url' : 'redis://localhost:6379/0',
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'expiration_time': 60*60*2,
        'distributed_lock': True
    }
    """
    def __init__(self, url=None, **kwargs):
        self.url = url or kwargs.pop('url', None)
        self.host = kwargs.pop('host', 'localhost')
        self.password = kwargs.pop('password', None)
        self.port = kwargs.pop('port', 6379)
        self.db = kwargs.pop('db', 0)
        self.distributed_lock = kwargs.get('distributed_lock', False)
        self.socket_timeout = kwargs.pop('socket_timeout', None)
        self.lock_timeout = kwargs.get('lock_timeout', None)
        self.lock_sleep = kwargs.get('lock_sleep', 0.1)
        self.expiration_time = kwargs.pop('expiration_time', 0)
        self.connection_pool = kwargs.get('connection_pool', None)
        self.client = self._create_client()

    def _create_client(self):
        if self.connection_pool is not None:
            # the connection pool already has all other connection
            # options present within, so here we disregard socket_timeout
            # and others.
            return redis.StrictRedis(connection_pool=self.connection_pool)

        args = {}
        if self.socket_timeout:
            args['socket_timeout'] = self.socket_timeout

        if self.url is not None:
            args.update(url=self.url)
            return redis.StrictRedis.from_url(**args)
        else:
            args.update(
                host=self.host, password=self.password,
                port=self.port, db=self.db
            )
            return redis.StrictRedis(**args)

    def get_mutex(self, key):
        if self.distributed_lock:
            return self.client.lock('_lock{0}'.format(key),
                                    self.lock_timeout, self.lock_sleep)
        else:
            return None

    def get(self, key):
        value = self.client.get(key)
        if value is None:
            return None
        return jsonutils.loads(value)

    def get_multi(self, keys):
        values = self.client.mget(keys)
        return [jsonutils.loads(v) if v is not None else None for v in values]

    def set(self, key, value):
        if self.expiration_time:
            self.client.setex(key, self.expiration_time,
                              jsonutils.dumps(value))
        else:
            self.client.set(key, jsonutils.dumps(value))

    def set_multi(self, mapping):
        mapping = dict(
            (k, jsonutils.dumps(v))
            for k, v in mapping.items()
        )

        if not self.expiration_time:
            self.client.mset(mapping)
        else:
            pipe = self.client.pipeline()
            for key, value in mapping.items():
                pipe.setex(key, self.expiration_time, value)
            pipe.execute()

    def delete(self, key):
        self.client.delete(key)

    def delete_multi(self, keys):
        self.client.delete(*keys)