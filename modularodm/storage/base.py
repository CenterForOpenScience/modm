import abc
import six
import time
import random
import logging
from functools import wraps

from ..translators import DefaultTranslator


logger = logging.getLogger(__name__)


class KeyExistsException(Exception):
    pass


def logify(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if self.log_level is None:
            return func(self, *args, **kwargs)
        start_time = time.time()
        ret = func(self, *args, **kwargs)
        stop_time = time.time()
        logger.log(
            self.log_level,
            'Called {0} on collection {1!r}: {2} s'.format(
                func.__name__,
                self,
                stop_time - start_time,
            )
        )
        return ret
    return wrapped


class StorageMeta(abc.ABCMeta):

    def __new__(mcs, name, bases, dct):

        # Decorate methods
        for key, value in dct.items():
            if hasattr(value, '__call__') \
                    and not isinstance(value, type) \
                    and not key.startswith('_'):
                dct[key] = logify(value)

        # Run super-metaclass __new__
        return super(StorageMeta, mcs).__new__(mcs, name, bases, dct)


@six.add_metaclass(StorageMeta)
class Storage(object):
    """Abstract base class for storage objects. Subclasses (e.g. PickleStorage,
    MongoStorage, etc.) must define insert, update, get, remove, flush, and
    find_all methods.
    """
    translator = DefaultTranslator()
    log_level = logging.INFO

    def __init__(self, log_level=None):
        self.log_level = log_level

    def _ensure_index(self, key):
        pass

    # todo allow custom id generator
    # todo increment n on repeated failures
    def _generate_random_id(self, n=5):
        """Generated random alphanumeric key.

        :param n: Number of characters in random key
        """
        alphabet = '23456789abcdefghijkmnpqrstuvwxyz'
        return ''.join(random.sample(alphabet, n))

    def _optimistic_insert(self, primary_name, value, n=5):
        """Attempt to insert with randomly generated key until insert
        is successful.

        :param str primary_name: The name of the primary key.
        :param dict value: The dictionary representation of the record.
        :param n: Number of characters in random key
        """
        while True:
            try:
                key = self._generate_random_id(n)
                value[primary_name] = key
                self.insert(primary_name, key, value)
                break
            except KeyExistsException:
                pass
        return key

    @abc.abstractmethod
    def insert(self, primary_name, key, value):
        '''Insert a new record.

        :param str primary_name: Name of primary key
        :param key: The value of the primary key
        :param dict value: The dictionary of attribute:value pairs
        '''
        pass

    @abc.abstractmethod
    def update(self, query, data):
        """Update multiple records with new data.

        :param query: A query object.
        :param dict data: Dictionary of key:value pairs.
        """
        pass

    @abc.abstractmethod
    def get(self, primary_name, key):
        """Get a single record.

        :param str primary_name: The name of the primary key.
        :param key: The value of the primary key.
        """
        pass

    @abc.abstractmethod
    def remove(self, query=None):
        """Remove records.
        """
        pass

    @abc.abstractmethod
    def flush(self):
        """Flush the database."""
        pass

    @abc.abstractmethod
    def find_one(self, query=None, **kwargs):
        """Find a single record that matches ``query``.
        """
        pass

    @abc.abstractmethod
    def find(self, query=None, **kwargs):
        """Query the database and return a query set.
        """
        pass
