from . import Field
from ..validators import validate_string

class StringField(Field):

    data_type = str
    validate = validate_string