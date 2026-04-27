class FakePipeline:
    def __init__(self, r):
        self.r = r
        self.ops = []
    
    def hgetall(self, key):
        self.ops.append(("hgetall", key))
        return self
    
    def hset(self, key, mapping):
        self.ops.append(("hset", key, mapping))
        return self
    
    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self
    
    def hmget(self, key, *fields):
        self.ops.append(("hmget", key, fields))
        return self

    def execute(self):
        out = []
        for op in self.ops:
            method = op[0]
            if method == "hgetall":
                out.append(dict(self.r.hashes.get(op[1], {})))
            elif method == "hset":
                key, mapping = op[1], op[2]
                self.r.hashes.setdefault(key, {})
                self.r.hashes[key].update({k: str(v) for k, v in mapping.items()})
                out.append(True)
            elif method == "expire":
                out.append(True)
            elif method == "hmget":
                key, fields = op[1], op[2]
                h = self.r.hashes.get(key, {})
                out.append([h.get(f) for f in fields])
        self.ops.clear()
        return out


class FakeRedis:
    def __init__(self):
        self.hashes: dict = {}
        self.kv: dict = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, field=None, value=None, mapping=None):
        """Supports hset(key, field, value) and hset(key, mapping={...})."""
        self.hashes.setdefault(key, {})
        if mapping is not None:
            self.hashes[key].update({k: str(v) for k, v in mapping.items()})
        elif field is not None:
            self.hashes[key][str(field)] = str(value) if value is not None else ""
        return True

    def get(self, key):
        v = self.kv.get(key)
        if v is None:
            return None
        return v if isinstance(v, bytes) else str(v).encode()

    def set(self, key, val, ex=None):
        self.kv[key] = val if isinstance(val, bytes) else str(val).encode()

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    def xgroup_create(self, *args, **kwargs):
        pass

    def xclaim(self, *args, **kwargs):
        return []

    def xreadgroup(self, *args, **kwargs):
        return []

    def xack(self, *args, **kwargs):
        pass

    def xadd(self, *args, **kwargs):
        pass
