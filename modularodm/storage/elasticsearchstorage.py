from elasticsearch import helpers

from .base import Storage
from ..query.queryset import BaseQuerySet
from ..query.query import QueryGroup
from ..query.query import RawQuery
from modularodm.exceptions import NoResultsFound, MultipleResultsFound

EQUALITY_OPERATORS = ('eq', 'ne')
SET_OPERATORS = ('in', 'nin')
RANGE_OPERATORS = ('gt', 'gte', 'lt', 'lte')
NEGATION_OPERATORS = ('ne', 'nin')

STRING_OPERATORS = ('contains', 'icontains', 'endswith')
STRINGOP_MAP = {'contains': '.*%s.*', 'icontains': '.*%s.*', 'endswith': '.*%s',}

class ElasticsearchQuerySet(BaseQuerySet):

    def __init__(self, schema, data):

        super(ElasticsearchQuerySet, self).__init__(schema)
        self.data = list(data)

    def __getitem__(self, index, raw=False):
        super(ElasticsearchQuerySet, self).__getitem__(index)
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

    def get_key(self, index):
        return self.__getitem__(index, raw=True)

    def get_keys(self):
        return list(self.__iter__(raw=True))

    def sort(self, *keys):

        sort_key = []
        for key in keys[::-1]:

            if key.startswith('-'):
                reverse = True
                key = key.lstrip('-')
            else:
                reverse = False

            self.data = sorted(self.data, key=lambda record: record[key], reverse=reverse)

        return self

    def offset(self, n):

        self.data = self.data[n:]
        return self

    def limit(self, n):

        self.data = self.data[:n]
        return self

class ElasticsearchStorage(Storage):

    QuerySet = ElasticsearchQuerySet

    def __init__(self, client, es_index, collection, ):
        self.client = client
        self.collection = collection
        self.es_index = es_index


    def find(self, query=None, **kwargs):
        elasticsearch_query = self._translate_query(query)

        matches = []
        for results in helpers.scan(
            self.client,
            query=elasticsearch_query,
            index=self.es_index,
            doc_type=self.collection,
        ):
            matches.append(results)


        return matches

    def find_one(self, query=None, **kwargs):
        """ Gets a single object from the collection.

        If no matching documents are found, raises ``NoResultsFound``.
        If >1 matching documents are found, raises ``MultipleResultsFound``.

        :params: One or more ``Query`` or ``QuerySet`` objects may be passed

        :returns: The selected document
        """
        elasticsearch_query = self._translate_query(query)
        matches = self.client.search(
            index=self.es_index,
            doc_type=self.collection,
            body=elasticsearch_query,
        )['hits']['hits']

        if len(matches) == 1:
            return matches[0]

        if len(matches) == 0:
            raise NoResultsFound()

        raise MultipleResultsFound(
            'Query for find_one must return exactly one result; '
            'returned {0}'.format(len(matches))
        )

    def get(self, primary_name, key):
        return self.client.get(index=self.es_index, doc_type=self.collection, id=key)

    def insert(self, primary_name, key, value):
        self.client.create(index=self.es_index, doc_type=self.collection, id=key, body=value)

    def update(self, query, data):
        for doc in self.find(query):
            self.client.update(
                index=self.es_index,
                doc_type=self.collection, id=doc['_id'],
                body={'doc': data}
            )

    def remove(self, query=None):
        elasticsearch_query = self._translate_query(query)
        self.client.delete_by_query(
            index=self.es_index,
            doc_type=self.collection,
            body=elasticsearch_query
        )

    def flush(self):
        pass

    def __repr__(self):
        return self.find()


    def _translate_query(self, query=None, elasticsearch_query=None):
        elasticsearch_query = self._build_query(query, elasticsearch_query)
        return {'filter' : elasticsearch_query}


    def _build_query(self, query=None, elasticsearch_query=None):
        elasticsearch_query = elasticsearch_query or {}

        if isinstance(query, RawQuery):
            attribute, operator, argument = \
                query.attribute, query.operator, query.argument

            if operator in EQUALITY_OPERATORS:
                elasticsearch_query['term'] = {attribute: argument}

            elif operator in RANGE_OPERATORS:
                elasticsearch_query['range'] = {attribute: {operator: argument}}

            elif operator in SET_OPERATORS:
                elasticsearch_query['terms'] = {attribute: argument}

            elif operator == 'startswith':
                elasticsearch_query['prefix'] = {attribute: argument}

            elif operator in STRING_OPERATORS:
                elasticsearch_query['regexp'] = {
                    attribute: self._stringop_to_regex(operator, argument)
                }


            if operator in NEGATION_OPERATORS:
                elasticsearch_query = {"not": elasticsearch_query}

        elif isinstance(query, QueryGroup):
            if query.operator == 'and':
                return {'and' : [self._build_query(node) for node in query.nodes]}

            elif query.operator == 'or':
                return {'or' : [self._build_query(node) for node in query.nodes]}

            elif query.operator == 'not':
                return {'not' : self._build_query(query.nodes[0])}

            else:
                raise ValueError('QueryGroup operator must be <and>, <or>, or <not>.')

        elif query is None:
            return {}

        else:
            raise TypeError('Query must be a QueryGroup or Query object.')

        return elasticsearch_query

    def _stringop_to_regex(self, operator, argument):
        return STRINGOP_MAP[operator] % argument

