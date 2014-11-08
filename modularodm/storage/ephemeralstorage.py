from .picklestorage import PickleStorage
from io import BytesIO
try:
    import cpickle as pickle
except ImportError:
    import pickle

class EphemeralStorage(PickleStorage):
    def __init__(self, *args, **kwargs):
        self.store = {}
        self.fp = BytesIO()

    def flush(self):
        pickle.dump(self.store, self.fp, -1)
