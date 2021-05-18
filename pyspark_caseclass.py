def CaseClass(case_class_name, **kwargs):
    types = {}
    for k, v in kwargs.items():
        if type(v) is not type:
            raise ValueError("{} is not a type".format(v))
        types[k] = v

    class CaseClassImpl():
        initialized = False
        def __init__(self, **kwargs):
            self._name = case_class_name
            self._keys = list(kwargs.keys())
            self._types = types

            expected_keys = set(self._types.keys())
            actual_keys = set(kwargs.keys())
            extra_keys = actual_keys.difference(expected_keys)
            missing_keys = expected_keys.difference(actual_keys)
            if missing_keys:
                raise ValueError("Missing values {}".format(missing_keys))
            
            if extra_keys:
                raise ValueError("Extra values {}".format(extra_keys))

            for k, v in kwargs.items():
                expected_type = self._types.get(k)
                actual_type = type(v)
                if actual_type is not expected_type:
                    raise ValueError("{} is of {}, must be of {}".format(k, actual_type, expected_type ))
                self.__setattr__(k, v)
            self.initialized = True

        def __repr__(self):
            inner = ", ".join(["{}={}".format(k, self.__getattribute__(k)) for k in self._keys])
            return "{}({})".format(self._name, inner)
        
        def __str__(self):
            return self.__repr__()

        def __eq__(self, other):
            return type(self) is type(other) and \
                self._name == other._name and \
                set(self._keys) == set(other._keys) and \
                all([other.__getattribute__(k) == self.__getattribute__(k) for k in self._keys])
        
        def __setattr__(self, name, value):
            if not self.initialized:
                return super.__setattr__(self, name, value)
            else:
                if name in self._keys:
                    raise AttributeError("Reassignment to val")
                else:
                    raise AttributeError("Value {} is no a member of {}".format(name, self._name))

        def copy(self, **kwargs):
            """ Returns a copy of the instance with optional overrides """
            orig = {k: self.__getattribute__(k) for k in self._keys}
            updated = {**orig, **kwargs}
            return CaseClassImpl(**updated)

        def match(self, *others):
            """ matches on a tuple of match keys and return values
                e.g. 
                    >>> case_class.match(
                        ({'key1': 'val1'}, "match on first!"),
                        ({'key1': 'val2', 'key2': 'val2'}, "match on second!"),
                        ({}, "match on thrid!"),
                    )
            """
            for match_key, match_val in others:
                if match_key is None or self.copy(**match_key) == self:
                    return match_val

    return CaseClassImpl