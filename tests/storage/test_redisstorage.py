#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import random
import datetime
import unittest
from nose.tools import *  # PEP8 asserts

import redis

from modularodm import StoredObject, fields
from modularodm.storage import RedisStorage
from modularodm.storage.redisstorage import RedisQuerySet
from modularodm.query.querydialect import DefaultQueryDialect as Q
from modularodm import exceptions

random.seed(1)


class Person(StoredObject):
    _meta = {"optimistic": True}
    _id = fields.StringField(primary=True, index=True)
    name = fields.StringField(required=True)
    age = fields.IntegerField(required=False)
    created = fields.DateTimeField(default=datetime.datetime.utcnow)

    def __repr__(self):
        return "<Person: {0!r}>".format(self.name)

class RedisTestCase(unittest.TestCase):
    # DB Settings
    DB_HOST = os.environ.get("REDIS_HOST", 'localhost')
    DB_PORT = os.environ.get("REDIS_PORT", 6379)

    client = redis.Redis(host=DB_HOST, port=DB_PORT)
    store = RedisStorage(client=client, collection='people')
    Person.set_storage(store)

    def tearDown(self):
        self.client.flushall()


class TestRedisStorage(RedisTestCase):

    def setUp(self):
        self.p1 = Person(name="Foo", age=12)
        self.p1.save()
        self.p2 = Person(name="Bar")
        self.p2.save()
        self.p3 = Person(name="Baz")
        self.p3.save()

    def test_insert(self):
        self.store.insert("_id", "abc123", {"name": "Steve", "age": 23})
        # Sets key => hash of attributes
        name = self.client.hget("people:abc123", "name")
        assert_equal(name, json.dumps("Steve"))
        age = int(self.client.hget("people:abc123", "age"))
        assert_equal(age, 23)

    def test_create_stored_object(self):
        p = Person(name="Foo", age=23)
        p.save()
        # has an _id
        assert_true(p._id)
        assert_equal(p.name, "Foo")
        assert_equal(p.age, 23)

    def test_load(self):
        p = Person(name="Foo")
        p.save()
        retrieved = Person.load(p._id)
        assert_equal(p, retrieved)

    def test_find_all(self):
        self.client.flushall()
        for i in range(5):
            p = Person(name="foo".format(i))
            p.save()
        all_people = Person.find()
        assert_equal(len(all_people), 5)
        assert_equal(all_people[0].name, 'foo')

    def test_find(self):
        retrieved = Person.find(Q("name", "eq", "Foo"))
        assert_in(self.p1, retrieved)
        assert_not_in(self.p2, retrieved)

    def test_find_by_pk(self):
        pks = list(self.store.find(by_pk=True))
        for each in Person.find():
            assert_in(each._primary_key, pks)

        pks = list(self.store.find(Q("name", "eq", self.p1.name), by_pk=True))
        assert_in(self.p1._primary_key, pks)

    def test_find_one(self):
        retrieved = Person.find_one(Q("name", "eq", "Foo"))
        assert_equal(self.p1, retrieved)

    def test_find_one_raises_error_if_no_records_found(self):
        p = Person(name="Foo")
        p.save()
        assert_raises(exceptions.NoResultsFound,
            lambda: Person.find_one(Q("name", "eq", "notfound")))

    def test_find_one_raises_error_if_multiple_records_found(self):
        self.client.flushall()
        p = Person(name="Foo")
        p.save()
        p2 = Person(name="Foo")
        p2.save()
        assert_raises(exceptions.MultipleResultsFound,
                    lambda: Person.find_one(Q("name", "eq", "Foo")))

    def test_repr(self):
        assert_equal(repr(self.store), "<RedisStorage: 'people'>")

    def test_get_key(self):
        assert_equal(self.store.get_key('abc123'), "people:abc123")

    def test_remove(self):
        all_people = list(Person.find())
        assert_in(self.p1, all_people)
        Person.remove_one(self.p1._primary_key)
        all_people = list(Person.find())
        assert_not_in(self.p1, all_people)
        # Keys were removed
        assert_not_in(self.p1._primary_key, self.store.get_key_set())
        redis_key = self.store.get_key(self.p1._primary_key)
        assert_not_in(redis_key, self.client.keys())

    def test_get_key_set(self):
        key_set = self.client.smembers("people_keys")
        assert_equal(self.store.get_key_set(), key_set)

    def test_update(self):
        query = Q("_id", "eq", self.p1._id)
        self.store.update(query, {"name": "Boo", 'age': 23})
        # Record as dict
        record = self.client.hgetall(self.store.get_key(self.p1._id))
        assert_equal(record['name'], json.dumps("Boo"))
        assert_equal(record['age'],json.dumps(23))

    def test_update_one_stored_object(self):
        Person.update_one(self.p1, {"name": "Boo"})
        assert_equal(self.p1.name, "Boo")

    def test_updating_pk(self):
        p = Person(name="Steve")
        old_key = p._primary_key
        p._id = 'mykey'
        p.save()
        StoredObject._clear_caches()
        assert_true(Person.load("mykey") is not None)
        assert_true(Person.load(old_key) is None)
        assert_equal(p._id, 'mykey')

    def test_update_multiple(self):
        self.client.flushall()
        recs = []
        for _ in range(5):
            p = Person(name="Foo")
            recs.append(p)
            p.save()
        self.store.update(Q("name", "eq", "Foo"), {"name": "Boo"})
        for rec in recs:
            rec.reload()
            assert_equal(rec.name, "Boo")


class TestRedisQuerySet(RedisTestCase):

    def setUp(self):
        for i in range(5):
            p = Person(name="Foo", age=i + 1)
            p.save()
        Person(name="Bar", age=6).save()
        self.qs = Person.find()

    def test_sort(self):
        sorted_qs = self.qs.sort("age")
        loaded_objects = [p for p in sorted_qs]
        expected = sorted(list(Person.find()), key=lambda rec: rec.age)
        assert_equal(loaded_objects, expected)

    def test_sort_reversed(self):
        sorted_qs = self.qs.sort("-age")
        loaded_objects = [p for p in sorted_qs]
        expected = sorted(list(Person.find()), key=lambda rec: rec.age, reverse=True)
        assert_equal(loaded_objects, expected)

    def test_limit(self):
        limited = self.qs.sort("age").limit(3)
        assert_equal(len(limited), 3)


if __name__ == '__main__':
    unittest.main()
