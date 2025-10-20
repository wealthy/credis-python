from typing import (
    Any,
    Awaitable,
    Callable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Set,
    Union,
)

import dill as pickle
from redis import Redis, Sentinel
from redis.typing import (
    EncodableT,
    ResponseT,
    KeyT,
    PatternT,
    ExpiryT,
    AbsExpiryT,
    AnyKeyT,
    ZScoreBoundT,
)
from redis.client import Pipeline
from credis.exceptions import InitError, SentinelError


class Client:
    def __init__(
        self,
        host: str,
        port: str,
        app_prefix: str,
        password: Optional[str] = None,
        socket_timeout: float = 0.5,
        masterset_name: str = "mymaster",
    ):
        self.__host = host
        self.__port = port
        self.__app_prefix = app_prefix
        self.__password = password
        self.__socket_timeout = socket_timeout
        self.__masterset_name = masterset_name
        self.__sentinel = None
        self.__master = None
        self.__slave = None
        self.connected = False
        self.__connect()

    def __str__(self):
        return f"Client(host={self.__host}, port={self.__port}, password={self.__password})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if not isinstance(other, Client):
            return False
        return (
            self.__host == other.__host
            and self.__port == other.__port
            and self.__password == other.__password
        )

    def __ne__(self, other):
        if not isinstance(other, Client):
            return True
        return not self.__eq__(other)

    def __connect(self):
        try:
            self.__sentinel = Sentinel(
                [(self.__host, self.__port)],
                socket_timeout=self.__socket_timeout,
                sentinel_kwargs={
                    "password": self.__password,
                },
            )
        except Exception as e:
            raise SentinelError(
                f"Failed to connect to Redis Sentinel at {self.__host}:{self.__port}: {str(e)}"
            ) from e
        try:
            self.__sentinel.discover_master(self.__masterset_name)
            self.__sentinel.discover_slaves(self.__masterset_name)
            self.__master = self.__sentinel.master_for(
                self.__masterset_name,
                socket_timeout=self.__socket_timeout,
                password=self.__password,
            )
            self.__slave = self.__sentinel.slave_for(
                self.__masterset_name,
                socket_timeout=self.__socket_timeout,
                password=self.__password,
            )
            self.connected = True
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Redis master/slave for {self.__masterset_name}: {str(e)}"
            ) from e

    @property
    def master(self) -> Redis:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master

    @property
    def slave(self) -> Redis:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return self.__slave

    def make_key(self, name: KeyT) -> str:
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        elif isinstance(name, memoryview):
            name = name.tobytes().decode("utf-8")
        else:
            name = str(name)
        return f"{self.__app_prefix}:{name}"

    def write_pipeline(self, transaction: bool = True) -> Pipeline:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master.pipeline(transaction)

    def read_pipeline(self, transaction: bool = True) -> Pipeline:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return self.__slave.pipeline(transaction)

    def close(self):
        if self.__master:
            self.__master.close()
        if self.__slave:
            self.__slave.close()
        if self.__sentinel:
            self.__sentinel = None
        self.connected = False

    def ping(self) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master.ping()

    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: Union[ExpiryT, None] = None,
        px: Union[ExpiryT, None] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: Union[AbsExpiryT, None] = None,
        pxat: Union[AbsExpiryT, None] = None,
    ) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return self.__master.set(
            name=name,
            value=encoded_value,
            ex=ex,
            px=px,
            nx=nx,
            xx=xx,
            keepttl=keepttl,
            get=get,
            exat=exat,
            pxat=pxat,
        )

    def get(self, name: KeyT) -> Any:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        value = self.__slave.get(name)
        if value is None:
            return None
        return pickle.loads(value)

    def incr(self, name: KeyT, amount: int = 1) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.incr(name, amount)

    def decr(self, name: KeyT, amount: int = 1) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.decr(name, amount)

    def append(self, name: KeyT, value: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.append(name, value)

    def getrange(self, name: KeyT, start: int, end: int) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.getrange(name, start, end)

    def setrange(self, name: KeyT, offset: int, value: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.setrange(name, offset, value)

    def strlen(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.strlen(name)

    def mget(self, keys: list[KeyT], *args: KeyT) -> list[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(str(k)) for k in keys] + [self.make_key(str(k)) for k in args]
        values = self.__slave.mget(all_keys)
        return [pickle.loads(v) if v is not None else None for v in values]

    def mset(self, mapping: dict[KeyT, EncodableT]) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        prefixed_mapping = {self.make_key(str(k)): pickle.dumps(v) for k, v in mapping.items()}
        return self.__master.mset(prefixed_mapping)

    def delete(self, *names: str) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        names_list = [self.make_key(str(name)) for name in names]
        return self.__master.delete(*names_list)

    def delete_raw(self, *names: str) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master.delete(*names)

    def lpush(self, name: KeyT, *values: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_values = [pickle.dumps(v) for v in values]
        return self.__master.lpush(name, *encoded_values)

    def rpush(self, name: KeyT, *values: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_values = [pickle.dumps(v) for v in values]
        return self.__master.rpush(name, *encoded_values)

    def lpop(self, name: KeyT, count: Optional[int] = None) -> Any:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        value = self.__master.lpop(name, count)
        if value is None:
            return None
        if isinstance(value, list):
            return [pickle.loads(v) for v in value]
        return pickle.loads(value)

    def rpop(self, name: KeyT, count: Optional[int] = None) -> Any:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        value = self.__master.rpop(name, count)
        if value is None:
            return None
        if isinstance(value, list):
            return [pickle.loads(v) for v in value]
        return pickle.loads(value)

    def lrange(self, name: KeyT, start: int, end: int) -> list[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        values = self.__slave.lrange(name, start, end)
        return [pickle.loads(v) for v in values]

    def llen(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.llen(name)

    def lindex(self, name: KeyT, index: int) -> Any:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        value = self.__slave.lindex(name, index)
        return pickle.loads(value) if value is not None else None

    def lset(self, name: KeyT, index: int, value: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__master.lset(name, index, encoded_value)

    def lrem(self, name: KeyT, count: int, value: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__master.lrem(name, count, encoded_value)

    def ltrim(self, name: KeyT, start: int, end: int) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.ltrim(name, start, end)

    def exists(self, *names: str) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        names_list = [self.make_key(str(name)) for name in names]
        return self.__slave.exists(*names_list)

    def keys(self, pattern: PatternT = "*") -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        pattern = f"{self.__app_prefix}:{str(pattern)}"
        return self.__slave.keys(pattern)

    def hset(
        self,
        name: KeyT,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        mapping: Optional[dict] = None,
        items: Optional[list] = None,
    ) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        # Encode value if provided
        encoded_value = pickle.dumps(value) if value is not None else None
        # Encode mapping values if provided
        encoded_mapping = {k: pickle.dumps(v) for k, v in mapping.items()} if mapping else None
        # Encode items values if provided (items is list of key-value pairs)
        encoded_items = [(k, pickle.dumps(v)) for k, v in items] if items else None
        return self.__master.hset(name, key, encoded_value, encoded_mapping, encoded_items)

    def hget(self, name: KeyT, key: str) -> Any:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        value = self.__slave.hget(name, key)
        return pickle.loads(value) if value is not None else None

    def hgetall(self, name: KeyT) -> dict[str, Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.hgetall(name)
        return {
            k.decode() if isinstance(k, bytes) else k: pickle.loads(v) for k, v in result.items()
        }

    def hdel(self, name: KeyT, *keys: str) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.hdel(name, *keys)

    def hkeys(self, name: KeyT) -> Union[Awaitable[list], list]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.hkeys(name)

    def hvals(self, name: KeyT) -> list[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        values = self.__slave.hvals(name)
        return [pickle.loads(v) for v in values]

    def hlen(self, name: KeyT) -> Union[Awaitable[int], int]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.hlen(name)

    def hexists(self, name: KeyT, key: str) -> Union[Awaitable[bool], bool]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.hexists(name, key)

    def hincrby(self, name: KeyT, key: str, amount: int = 1) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.hincrby(name, key, amount)

    def hincrbyfloat(
        self, name: KeyT, key: str, amount: float = 1.0
    ) -> Union[Awaitable[float], float]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.hincrbyfloat(name, key, amount)

    def hmget(self, name: KeyT, keys: list[str], *args: str) -> list[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        values = self.__slave.hmget(name, keys, *args)
        return [pickle.loads(v) if v is not None else None for v in values]

    def hsetnx(self, name: KeyT, key: str, value: Any) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__master.hsetnx(name, key, encoded_value)

    def hstrlen(self, name: KeyT, key: str) -> Union[Awaitable[int], int]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.hstrlen(name, key)

    def hrandfield(
        self, name: KeyT, count: Optional[int] = None, withvalues: bool = False
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.hrandfield(name, count, withvalues)
        if withvalues:
            # Result is list of [key, value, key, value, ...]
            decoded_result = []
            for i in range(0, len(result), 2):
                key = result[i].decode() if isinstance(result[i], bytes) else result[i]
                value = pickle.loads(result[i + 1])
                decoded_result.extend([key, value])
            return decoded_result
        else:
            # Result is list of keys (no need to decode keys)
            return result

    def hscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        cursor, result = self.__slave.hscan(name, cursor, match, count, no_values)
        # Decode values in the result dictionary
        decoded_result = {
            k.decode() if isinstance(k, bytes) else k: pickle.loads(v) for k, v in result.items()
        }
        return cursor, decoded_result

    def hscan_iter(
        self,
        name: KeyT,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        iterator = self.__slave.hscan_iter(name, match, count, no_values)
        # Wrap iterator to decode values
        for key, value in iterator:
            yield (key.decode() if isinstance(key, bytes) else key, pickle.loads(value))

    def flushdb(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master.flushdb(asynchronous=asynchronous, **kwargs)

    def flushall(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return self.__master.flushall(asynchronous=asynchronous, **kwargs)

    def scan(
        self,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs: Any,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return self.__slave.scan(cursor, match, count, _type, **kwargs)

    def scan_iter(
        self,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return self.__slave.scan_iter(match, count, _type, **kwargs)

    def sadd(self, name: KeyT, *values: Any) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_values = [pickle.dumps(v) for v in values]
        return self.__master.sadd(name, *encoded_values)

    def srem(self, name: KeyT, *values: Any) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_values = [pickle.dumps(v) for v in values]
        return self.__master.srem(name, *encoded_values)

    def smembers(self, name: KeyT) -> Set[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        members = self.__slave.smembers(name)
        return {pickle.loads(m) for m in members}

    def sismember(
        self, name: KeyT, value: Any
    ) -> Union[Awaitable[Union[Literal[0], Literal[1]]], Union[Literal[0], Literal[1]]]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__slave.sismember(name, encoded_value)

    def smove(self, src: str, dst: str, value: Any) -> Union[Awaitable[bool], bool]:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        src = self.make_key(str(src))
        dst = self.make_key(str(dst))
        encoded_value = pickle.dumps(value)
        return self.__master.smove(src, dst, encoded_value)

    def scard(self, name: KeyT) -> Union[Awaitable[int], int]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.scard(name)

    def sdiff(self, keys: list[KeyT], *args: KeyT) -> Set[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(str(k)) for k in keys] + [self.make_key(str(k)) for k in args]
        members = self.__slave.sdiff(all_keys)
        return {pickle.loads(m) for m in members}

    def sinter(self, keys: list[KeyT], *args: KeyT) -> Set[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(str(k)) for k in keys] + [self.make_key(str(k)) for k in args]
        members = self.__slave.sinter(all_keys)
        return {pickle.loads(m) for m in members}

    def sunion(self, keys: list[KeyT], *args: KeyT) -> Set[Any]:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(str(k)) for k in keys] + [self.make_key(str(k)) for k in args]
        members = self.__slave.sunion(all_keys)
        return {pickle.loads(m) for m in members}

    def spop(self, name: KeyT, count: Optional[int] = None) -> Any:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__master.spop(name, count)
        if result is None:
            return None
        if isinstance(result, list):
            return [pickle.loads(m) for m in result]
        else:
            return pickle.loads(result)

    def srandmember(self, name: KeyT, number: Optional[int] = None) -> Any:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.srandmember(name, number)
        if result is None:
            return None
        if isinstance(result, list):
            return [pickle.loads(m) for m in result]
        else:
            return pickle.loads(result)

    def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        cursor, members = self.__slave.sscan(name, cursor, match, count)
        # Decode members
        decoded_members = [pickle.loads(m) for m in members]
        return cursor, decoded_members

    def sscan_iter(
        self,
        name: KeyT,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        iterator = self.__slave.sscan_iter(name, match, count)
        # Wrap iterator to decode members
        for member in iterator:
            yield pickle.loads(member)

    def zadd(
        self,
        name: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_mapping = {k: pickle.dumps(v) for k, v in mapping.items()}
        return self.__master.zadd(
            name=name, mapping=encoded_mapping, nx=nx, xx=xx, ch=ch, incr=incr, gt=gt, lt=lt
        )

    def zrem(self, name: KeyT, *values: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_values = [pickle.dumps(v) for v in values]
        return self.__master.zrem(name, *encoded_values)

    def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: Optional[int] = None,
        num: Optional[int] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.zrange(
            name=name,
            start=start,
            end=end,
            desc=desc,
            withscores=withscores,
            score_cast_func=score_cast_func,
            byscore=byscore,
            bylex=bylex,
            offset=offset,  # type: ignore
            num=num,  # type: ignore
        )
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    def zrevrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.zrevrange(
            name=name,
            start=start,
            end=end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: Union[int, None] = None,
        num: Union[int, None] = None,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.zrangebyscore(name, min, max, start, num, withscores, score_cast_func)
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    def zrevrangebyscore(
        self,
        name: KeyT,
        max: ZScoreBoundT,
        min: ZScoreBoundT,
        start: Union[int, None] = None,
        num: Union[int, None] = None,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__slave.zrevrangebyscore(
            name, max, min, start, num, withscores, score_cast_func
        )
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    def zcard(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.zcard(name)

    def zcount(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.zcount(name, min, max)

    def zrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__slave.zrank(name, encoded_value, withscore)

    def zrevrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__slave.zrevrank(name, encoded_value, withscore)

    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        cursor, result = self.__slave.zscan(
            name=name,
            cursor=cursor,
            match=match,
            count=count,
            score_cast_func=score_cast_func,
        )
        # Decode members in the result (list of (member, score) tuples)
        decoded_result = [(pickle.loads(member), score) for member, score in result]
        return cursor, decoded_result

    def zscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        iterator = self.__slave.zscan_iter(
            name=name, match=match, count=count, score_cast_func=score_cast_func
        )
        # Wrap iterator to decode members
        for member, score in iterator:
            yield (pickle.loads(member), score)

    def zremrangebyrank(self, name: KeyT, min: int, max: int) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.zremrangebyrank(name, min, max)

    def zscore(self, name: KeyT, value: EncodableT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__slave.zscore(name, encoded_value)

    def zincrby(self, name: KeyT, amount: float, value: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        encoded_value = pickle.dumps(value)
        return self.__master.zincrby(name, amount, encoded_value)

    def zpopmin(self, name: KeyT, count: Optional[int] = None) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__master.zpopmin(name, count)
        if result:
            return [(pickle.loads(member), score) for member, score in result]
        return result

    def zpopmax(self, name: KeyT, count: Optional[int] = None) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        result = self.__master.zpopmax(name, count)
        if result:
            return [(pickle.loads(member), score) for member, score in result]
        return result

    def zremrangebyscore(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.zremrangebyscore(name, min, max)

    def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: Optional[int] = None,
        num: Optional[int] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.zrangebylex(name, min, max, start, num)

    def expire(self, name: KeyT, time: ExpiryT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.expire(name, time)

    def expireat(self, name: KeyT, when: AbsExpiryT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.expireat(name, when)

    def ttl(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.ttl(name)

    def pttl(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.pttl(name)

    def persist(self, name: KeyT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__master.persist(name)

    def rename(self, src: KeyT, dst: KeyT) -> ResponseT:
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        src = self.make_key(str(src))
        dst = self.make_key(str(dst))
        return self.__master.rename(src, dst)

    def type(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(str(name))
        return self.__slave.type(name)
