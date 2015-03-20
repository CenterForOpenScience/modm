import mock
from nose.tools import *  # noqa

from modularodm import StoredObject
from modularodm.fields import ForeignField, IntegerField

from tests.base import ModularOdmTestCase


class LazyLoadTestCase(ModularOdmTestCase):

    def define_objects(self):

        class Foo(StoredObject):
            _id = IntegerField()
            my_bar = ForeignField('Bar', list=True, backref='my_foo')

        class Bar(StoredObject):
            _id = IntegerField()

        return Foo, Bar

    def test_load_object_in_cache(self):

        bar = self.Bar(_id=1)
        bar.save()

        with mock.patch.object(self.Bar._storage[0], 'get') as mock_load:
            self.Bar.load(1)
        assert_false(mock_load.called)

    def test_load_object_not_in_cache(self):

        bar = self.Bar(_id=1)
        bar.save()

        self.Bar._clear_caches(1)

        with mock.patch.object(self.Bar._storage[0], 'get') as mock_load:
            self.Bar.load(1)
        assert_true(mock_load.called)

    def test_create_several_objects(self):

        with mock.patch.object(self.Bar._storage[0], 'insert') as mock_insert:

            bar1 = self.Bar(_id=1)
            bar2 = self.Bar(_id=2)
            bar3 = self.Bar(_id=3)
            bar4 = self.Bar(_id=4)
            bar5 = self.Bar(_id=5)

            bar1.save()
            bar2.save()
            bar3.save()
            bar4.save()
            bar5.save()

        assert_equal(len(mock_insert.call_args_list), 5)

    def test_create_linked_objects(self):

        bar1 = self.Bar(_id=1)
        bar2 = self.Bar(_id=2)
        bar3 = self.Bar(_id=3)

        bar1.save()
        bar2.save()
        bar3.save()

        with mock.patch.object(self.Foo._storage[0], 'insert') as mock_insert:
            with mock.patch.object(self.Bar._storage[0], 'update') as mock_update:

                foo1 = self.Foo(_id=4)
                foo1.my_bar = [bar1, bar2, bar3]
                foo1.save()

        assert_equal(len(mock_insert.call_args_list), 1)
        assert_equal(len(mock_update.call_args_list), 3)

    def test_load_linked_objects_not_in_cache(self):

        bar1 = self.Bar(_id=1)
        bar2 = self.Bar(_id=2)
        bar3 = self.Bar(_id=3)

        bar1.save()
        bar2.save()
        bar3.save()

        foo1 = self.Foo(_id=4)
        foo1.my_bar = [bar1, bar2, bar3]
        foo1.save()

        StoredObject._clear_caches()

        with mock.patch.object(self.Foo._storage[0], 'get') as mock_foo_get:
            with mock.patch.object(self.Bar._storage[0], 'get') as mock_bar_get:
                self.Foo.load(4)

        assert_true(mock_foo_get.called)
        assert_false(mock_bar_get.called)
