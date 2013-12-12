# -*- coding: utf-8 -*-
from bson import json_util
import json


def dumps(val):
    '''Custom JSON serialization function that allows serialization of special
    types, e.g. datetimes, UUIDs, etc.
    '''
    return json.dumps(val, default=json_util.default)


def loads(val):
    '''Custom JSON deserialization function that handles special types.'''
    return json.loads(val, object_hook=json_util.object_hook)
