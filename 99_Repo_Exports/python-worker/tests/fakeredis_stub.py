class FakeRedis:
    def __init__(self, decode_responses=False, **kwargs):
        self._data = {}
        self._lists = {}  # Для lrange, rpush
        self.decode_responses = decode_responses

    def hset(self, name, key=None, value=None, mapping=None):
        if name not in self._data:
            self._data[name] = {}
        if mapping:
            self._data[name].update(mapping)
        else:
            self._data[name][key] = value
        return 1

    def hget(self, name, key):
        if name not in self._data:
            return None
        val = self._data[name].get(key)
        if val is None:
            return None
        return str(val) if self.decode_responses else val

    def hgetall(self, name):
        return self._data.get(name, {})

    def hmget(self, name, *keys):
        """Return values for multiple hash fields (in order)."""
        entry = self._data.get(name)
        if not isinstance(entry, dict):
            return [None] * len(keys)
        return [str(entry[k]) if (k in entry and self.decode_responses) else entry.get(k) for k in keys]

    def hdel(self, name, *keys):
        """Delete one or more hash fields."""
        if name not in self._data:
            return 0
        deleted = 0
        for key in keys:
            if key in self._data[name]:
                del self._data[name][key]
                deleted += 1
        return deleted

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        if nx and name in self._data:
            return False
        if xx and name not in self._data:
            return False
        self._data[name] = str(value) if self.decode_responses else value
        return True

    def get(self, name):
        val = self._data.get(name)
        if val is None:
            return None
        return str(val) if self.decode_responses else val

    def delete(self, *names):
        """Delete one or more keys; returns count of deleted keys."""
        count = 0
        for n in names:
            if n in self._data:
                del self._data[n]
                count += 1
            if n in self._lists:
                del self._lists[n]
                count += 1
        return count

    def pttl(self, name):
        """Return -1 (no TTL support in stub)."""
        return -1 if name in self._data else -2

    def scan_iter(self, match=None, count=None):
        """Yield keys matching a glob-style pattern (subset: only '*' wildcard)."""
        import fnmatch
        pattern = match or "*"
        for key in list(self._data.keys()):
            if fnmatch.fnmatch(key, pattern):
                yield key

    def rpush(self, name, *values):
        if name not in self._lists:
            self._lists[name] = []
        for v in values:
            self._lists[name].append(str(v) if self.decode_responses else v)
        return len(self._lists[name])

    def lrange(self, name, start, end):
        if name not in self._lists:
            return []
        lst = self._lists[name]
        if end == -1:
            end = len(lst)
        return lst[start:end+1]

    def expire(self, name, time):
        # Просто игнорируем expire для fake Redis
        return True

    def xadd(self, name, fields, maxlen=None, approximate=None):
        # Просто игнорируем xadd для fake Redis (для тестов)
        # Возвращаем fake message ID
        return "fake_msg_id"

    def pipeline(self):
        class _Pipeline:
            def __init__(self, r):
                self.r = r
                self.ops = []
            def hset(self, name, key, value):
                self.ops.append(('hset', (name, key, value)))
                return self
            def hdel(self, name, *keys):
                self.ops.append(('hdel', (name,) + keys))
                return self
            def set(self, name, value, ex=None):
                self.ops.append(('set', (name, value)))
                return self
            def get(self, name):
                self.ops.append(('get', (name,)))
                return self
            def delete(self, *names):
                self.ops.append(('delete', names))
                return self
            def execute(self):
                results = []
                for op, args in self.ops:
                    if op == 'hset':
                        result = self.r.hset(args[0], key=args[1], value=args[2])
                        results.append(result)
                    elif op == 'hdel':
                        result = self.r.hdel(args[0], *args[1:])
                        results.append(result)
                    elif op == 'set':
                        result = self.r.set(args[0], args[1])
                        results.append(result)
                    elif op == 'get':
                        result = self.r.get(args[0])
                        results.append(result)
                    elif op == 'delete':
                        result = self.r.delete(*args)
                        results.append(result)
                return results
        return _Pipeline(self)

    @property
    def connection_pool(self):
        class _Pool:
            connection_kwargs = {}
        return _Pool()


class FakeStrictRedis(FakeRedis):
    """Алиас для совместимости с fakeredis библиотекой."""
    pass
