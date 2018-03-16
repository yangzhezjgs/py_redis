class DataBase:
    data_type = None

    def __init__(self, data=None):
        self.data = {}
        self.value_type = data or self.data_type()

    def create_key(self, key): 
        self.data[key] = self.value_type

    def load(self):
        raise NotImplementedError

    def dump(self):
        raise NotImplementedError
