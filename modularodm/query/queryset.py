
# todo groupby
# todo indexing

class BaseQuerySet(object):

    def __init__(self, schema, data=None):

        self.schema = schema
        self.primary = schema._primary_name
        self.data = data

    def __getitem__(self, index):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def sort(self, *keys):
        raise NotImplementedError

    def offset(self, n):
        raise NotImplementedError

    def limit(self, n):
        raise NotImplementedError
