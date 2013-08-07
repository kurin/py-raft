def create_map():
    first = BijectiveMap()
    second = first.other
    return first, second

class BijectiveMap(dict):
    def __init__(self, other=None):
        if other is None:
            self.other = BijectiveMap(self)
        else:
            self.other = other

    def __setitem__(self, key, value):
        if key in self:
            # we want to do a -> b but we already have a mapping
            # a -> c, which means the other dict has c -> a
            # first delete c from the other dict, and then proceed            
            oldval = self[key]
            dict.__delitem__(self.other, oldval)
        if value in self.other:
            # we're doing a -> b but we already have c -> b
            # which destroys the bijection.  remove the key c
            oldkey = self.other[value]
            dict.__delitem__(self, oldkey)
        dict.__setitem__(self, key, value)
        dict.__setitem__(self.other, value, key)

    def __delitem__(self, key):
        val = self[key]
        dict.__delitem__(self, key)
        dict.__delitem__(self.other, val)
