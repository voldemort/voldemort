class OrderedDict(dict):
    def __init__(self, args=None, **kwargs):
        dict.__init__(self)
        self.keyList = []

        if args:
            if isinstance(args, dict):
                for k, v in args.iteritems():
                    self[k] = v
            else:
                for k, v in args:
                    self[k] = v

        if kwargs:
            for k, v in kwargs.iteritems():
                self[k] = v


    def keys(self):
        return self.keyList

    def iterkeys(self):
        return iter(self.keyList)

    def __iter__(self):
        return iter(self.keyList)

    def items(self):
        return [(k, self[k]) for k in self.iterkeys()]

    def iteritems(self):
        for k in self.iterkeys():
            yield (k, self[k])

    def itervalues(self):
        for k in self.iterkeys():
            yield self[k]

    def values(self):
        return [self[k] for k in self.iterkeys()]

    def __setitem__(self, k, v):
        if dict.__contains__(self, k):
            self.keyList.remove(k)
        self.keyList.append(k)
        dict.__setitem__(self, k, v)

    def __delitem__(self, k):
        if dict.__contains__(self, k):
            self.keyList.remove(k)

        dict.__delitem__(self, k)

    def __str__(self):
        tokens = ['{']
        for k, v in self.iteritems():
            if len(tokens) > 1:
                tokens.append(', ')
            tokens.append(repr(k))
            tokens.append(':')
            tokens.append(repr(v))

        tokens.append('}')
        return ''.join(tokens)

    def __repr__(self):
        return str(self)


