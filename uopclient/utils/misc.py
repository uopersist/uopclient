def values_from_keys(a_dict):
    return lambda keys: [a_dict[k] for k in keys]

def get_by_id(source, db_fun):
    def inner(_id):
        if id not in source:
            thing = db_fun(id)
            if thing:
                source[id] = thing
        return source.get(id)
    return inner

class db_hash_get:
    def __init__(self, source_dict, db_getter):
        self._source = source_dict
        self._getter = db_getter

    def __call__(self, key):
        if key not in self._source:
            value = self._getter(key)
            if value:
                self._source[key] = value
        return self._source.get(key)