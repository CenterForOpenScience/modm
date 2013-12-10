# -*- coding: utf-8 -*-

from .base import Storage
from ..query.query import QueryGroup, RawQuery
from ..query.queryset import BaseQuerySet
from .picklestorage import operators


class RedisQuerySet(BaseQuerySet):

    def __init__(self, schema, data):
        super(RedisQuerySet,  self).__init__(schema, data)
        self.data = list(data)

    def __getitem__(self, index, raw=False):
        super(RedisQuerySet, self).__getitem__(index)
        key = self.data[index][self.primary]
        if raw:
            return key
        return self.schema.load(key)

    def __iter__(self, raw=False):
        keys = [obj[self.primary] for obj in self.data]
        if raw:
            return keys
        return (self.schema.load(key) for key in keys)

    def __len__(self):
        return len(self.data)

    count = __len__

    def __repr__(self):
        return "<RedisQuerySet: {0}>".format(repr(list(self.data)))


class RedisStorage(Storage):
    '''Storage backend for Redis. Requires redis-py.

    Each record is stored as a Hash keyed by the string:
        <collection>:<id>

    In addition a set keyed by <collection>_keys stores a set of all primary
    keys for the collection.

    :param redis.Redis client: The ``redis.Redis`` object from redis-py.
    :param str collection: The name of the collection, e.g. "user"
    '''
    QuerySet = RedisQuerySet

    def __init__(self, client, collection):
        self.client = client
        self.collection = collection
        #: Name of set that stores the primary keys for this collection
        self._key_set = "{col}_keys".format(col=self.collection)

    def get(self, primary_name, key):
        """Get a record as a dictionary."""
        record = self.client.hgetall("{col}:{pk}".format(col=self.collection, pk=key))
        return record

    def get_by_id(self, id):
        return self.get(None, id)

    def insert(self, primary_name, key, value):
        '''Insert a new record.

        :param str primary_name: Name of primary key
        :param key: The value of the primary key
        :param dict value: The dictionary of attribute:value pairs
        '''
        if primary_name not in value:
            value = value.copy()
            value[primary_name] = key
        # Add to set of primary keys
        self.client.sadd(self._key_set, key)
        # <collection>:<primary_key> => Hash of attribute:value pairs
        self.client.hmset("{col}:{pk}".format(col=self.collection, pk=key),
                        value)
        return None

    def _match(self, name, query):
        """Return whether the Hash named ``name`` matches the ``query``.
        """
        # TODO: Duplication of PickleStorage logic. Rethink.
        if isinstance(query, QueryGroup):
            matches = [self._match(name, node) for node in query.nodes]

            if query.operator == 'and':
                return all(matches)
            elif query.operator == 'or':
                return any(matches)
            elif query.operator == 'not':
                return not any(matches)
            else:
                raise ValueError('QueryGroup operator must be <and>, <or>, or <not>.')

        elif isinstance(query, RawQuery):
            attribute, operator, argument = \
                query.attribute, query.operator, query.argument
            attribute_value = self.client.hget(name, attribute)
            # Use same operators as pickle storage
            comp_function = operators[operator]
            return comp_function(attribute_value, argument)
        else:
            raise TypeError('Query must be a QueryGroup or Query object.')

    def find(self, query=None, by_pk=False):
        """Return generator over query results. Takes optional
        by_pk keyword argument; if True, return keys rather than
        values.
        """
        if query is None:
            # Yield every object in the collection
            for primary_key in self.client.smembers(self._key_set):
                yield self.get_by_id(primary_key)
        else:
            for primary_key in self.client.smembers(self._key_set):
                # The hash name
                name = "{col}:{pk}".format(col=self.collection, pk=primary_key)
                if self._match(name, query):
                    if by_pk:
                        yield primary_key
                    else:
                        record = self.get_by_id(primary_key)
                        yield record

    def __repr__(self):
        return "<RedisStorage: {0!r}>".format(self.collection)
