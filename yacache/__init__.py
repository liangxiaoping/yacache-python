#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Author: liangxiaoping
#

from yacache.client import RedisClient

CLIENT = None


def get_client(url=None, **kwargs):
    global CLIENT
    if CLIENT is None:
        CLIENT = RedisClient(url=url, **kwargs)
    return CLIENT
