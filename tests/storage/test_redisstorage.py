#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import unittest
from nose.tools import *  # PEP8 asserts

import redis

from modularodm import StoredObject, fields
from modularodm.storage import RedisStorage
from modularodm.query.querydialect import DefaultQueryDialect as Q
from modularodm import exceptions

class Person(StoredObject):
    _meta = {"optimistic": True}
    _id = fields.StringField(primary=True, index=True)
    name = fields.StringField(required=True)

    def __repr__(self):
        return "<Person: {0}>".format(self.name)

class TestRedisStorage(unittest.TestCase):

    # DB Settings
    DB_HOST = os.environ.get("REDIS_HOST", 'localhost')
    DB_PORT = os.environ.get("REDIS_PORT", 6379)

    client = redis.Redis(host=DB_HOST, port=DB_PORT)
    Person.set_storage(RedisStorage(client=client, collection='people'))

    def tearDown(self):
        self.client.flushall()

    def test_insert(self):
        p = Person(name="Foo")
        p.save()
        # has an _id
        assert_true(p._id)

    def test_load(self):
        p = Person(name="Foo")
        p.save()
        retrieved = Person.load(p._id)
        assert_equal(p, retrieved)

    def test_find_all(self):
        for i in range(5):
            p = Person(name="foo".format(i))
            p.save()
        all_people = Person.find()
        assert_equal(len(all_people), 5)
        assert_equal(all_people[0].name, 'foo')

    def test_find(self):
        p = Person(name="Foo")
        p.save()
        p2 = Person(name="Bar")
        p2.save()
        retrieved = Person.find(Q("name", "eq", "Foo"))
        assert_in(p, retrieved)
        assert_not_in(p2, retrieved)

    def test_find_one(self):
        p = Person(name="Foo")
        p.save()
        p2 = Person(name="Bar")
        p2.save()
        retrieved = Person.find_one(Q("name", "eq", "Foo"))
        assert_equal(p, retrieved)

    def test_find_one_raises_error_if_no_records_found(self):
        p = Person(name="Foo")
        p.save()
        assert_raises(exceptions.NoResultsFound,
            lambda: Person.find_one(Q("name", "eq", "Bar")))

    def test_find_one_raises_error_if_multiple_records_found(self):
        p = Person(name="Foo")
        p.save()
        p2 = Person(name="Foo")
        p2.save()
        assert_raises(exceptions.MultipleResultsFound,
                    lambda: Person.find_one(Q("name", "eq", "Foo")))

    def test_repr(self):
        assert_equal(repr(Person._storage[0]), "<RedisStorage: 'people'>")

if __name__ == '__main__':
    unittest.main()
