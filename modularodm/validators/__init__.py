import six
from six.moves.urllib_parse import urlsplit, urlunsplit

from modularodm.exceptions import (
    ValidationError,
    ValidationTypeError,
    ValidationValueError,
)

from bson import ObjectId

class Validator(object):

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class TypeValidator(Validator):

    def _as_list(self, value):

        if isinstance(value, (tuple, list)):
            return value
        return [value]

    def __init__(self, allowed_types, forbidden_types=None):
        self.allowed_types = self._as_list(allowed_types)
        self.forbidden_types = self._as_list(forbidden_types) if forbidden_types else []

    def __call__(self, value):

        for ftype in self.forbidden_types:
            if isinstance(value, ftype):
                self._raise(value)

        for atype in self.allowed_types:
            if isinstance(value, atype):
                return

        self._raise(value)

    def _raise(self, value):

        raise ValidationTypeError(
            'Received invalid value {} of type {}'.format(
                value, type(value)
            )
        )

validate_string = TypeValidator(six.string_types)
validate_integer = TypeValidator(
    allowed_types=int,
    forbidden_types=bool
)
validate_float = TypeValidator(float)
validate_boolean = TypeValidator(bool)
validate_objectid = TypeValidator(ObjectId)

from ..fields.lists import List
validate_list = TypeValidator(List)

import datetime
validate_datetime = TypeValidator(datetime.datetime)

# Adapted from Django RegexValidator
import re
class RegexValidator(Validator):

    def __init__(self, regex=None, flags=0):

        if regex is not None:
            self.regex = re.compile(regex, flags=flags)

    def __call__(self, value):

        if not self.regex.findall(value):
            raise ValidationError(
                'Value must match regex {0} and flags {1}; received value <{2}>'.format(
                    self.regex.pattern,
                    self.regex.flags,
                    value
                )
            )

# Adapted from Django URLValidator
class URLValidator(RegexValidator):
    regex = re.compile(
        ur'^(?:(?:https?|ftp)://)?'  # http:// or https://
        ur'(?:\S+(?::\S*)?@)?'  # user:passauthentication
        ur'(?:(?:(?:[\u00a1-\uffffA-Z0-9][\u00a1-\uffffA-Z0-9-]{0,61}[\u00a1-\uffffA-Z0-9]?\.)+(?:[\u00a1-\uffffA-Z0-9]{2,}))|'  # domain...
        ur'localhost|'  # localhost...
        ur'(?:(?:(?:25[0-5]|2[0-4][0-9]|[1]?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|[1]?[0-9]?[0-9]))|' #...or ipv4
        ur'(?:\[?[A-F0-9]*:[A-F0-9:]+\]?))'  # ...or ipv6
        ur'(?::\d{2,5})?'  # optional port
        ur'(?:/|/\S+)*$', re.IGNORECASE)
    # message = _('Enter a valid URL.')

    def __call__(self, value):
        try:
            super(URLValidator, self).__call__(value)
        except ValidationError as e:
                raise e
        else:
            pass


class BaseValidator(Validator):

    compare = lambda self, a, b: a is not b
    clean = lambda self, x: x
    message = 'Ensure this value is %(limit_value)s (it is %(show_value)s).'
    code = 'limit_value'

    def __init__(self, limit_value):
        self.limit_value = limit_value

    def __call__(self, value):
        cleaned = self.clean(value)
        params = {'limit_value': self.limit_value, 'show_value': cleaned}
        if self.compare(cleaned, self.limit_value):
            raise ValidationValueError(self.message.format(**params))


class MaxValueValidator(BaseValidator):

    compare = lambda self, a, b: a > b
    message = 'Ensure this value is less than or equal to {limit_value}.'
    code = 'max_value'


class MinValueValidator(BaseValidator):

    compare = lambda self, a, b: a < b
    message = 'Ensure this value is greater than or equal to {limit_value}.'
    code = 'min_value'


class MinLengthValidator(BaseValidator):

    compare = lambda self, a, b: a < b
    clean = lambda self, x: len(x)
    message = 'Ensure this value has length of at least {limit_value} (it has length {show_value}).'
    code = 'min_length'


class MaxLengthValidator(BaseValidator):

    compare = lambda self, a, b: a > b
    clean = lambda self, x: len(x)
    message = 'Ensure this value has length of at most {limit_value} (it has length {show_value}).'
    code = 'max_length'
