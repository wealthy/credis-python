from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Literal,
    Mapping,
    Optional,
    Set,
    Union,
)

import dill as pickle
from redis.asyncio import Redis, Sentinel
from redis.asyncio.client import Pipeline
from redis.typing import (
    AbsExpiryT,
    AnyKeyT,
    EncodableT,
    ExpiryT,
    FieldT,
    KeyT,
    PatternT,
    ResponseT,
    ZScoreBoundT,
)

from credis.exceptions import InitError


class AsyncClient:
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
        self.__sentinel: Sentinel | None = None
        self.__master: Optional[Redis] = None
        self.__slave: Optional[Redis] = None
        self.connected = False

    async def connect(self):
        """Connect to the Redis Sentinel and get the master and slave addresses."""

        self.__sentinel = Sentinel(
            [(self.__host, self.__port)],
            socket_timeout=self.__socket_timeout,
            sentinel_kwargs={"password": self.__password},
        )
        try:
            await self.__sentinel.discover_master(self.__masterset_name)
            await self.__sentinel.discover_slaves(self.__masterset_name)
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
            raise ConnectionError(f"Failed to connect to Redis master/slave: {e}") from e

    def make_key(self, name: KeyT) -> str:
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        elif isinstance(name, memoryview):
            name = name.tobytes().decode("utf-8")
        else:
            name = str(name)
        return f"{self.__app_prefix}:{name}"

    async def write_pipeline_ctx(self, transaction: bool = True) -> AsyncIterator[Pipeline]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        async with self.__master.pipeline(transaction) as pipe:
            yield pipe

    async def write_pipeline(self, transaction: bool = True) -> Pipeline:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return await self.__master.pipeline(transaction)

    async def read_pipeline_ctx(self) -> AsyncIterator[Pipeline]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        async with self.__slave.pipeline() as pipe:
            yield pipe

    async def read_pipeline(self) -> Pipeline:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return await self.__slave.pipeline()

    async def transaction(
        self,
        func: Callable[[Pipeline], Union[Any, Awaitable[Any]]],
        *watches: KeyT,
        shard_hint: Optional[str] = None,
        value_from_callable: bool = False,
        watch_delay: Optional[float] = None,
    ):
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        await self.__master.transaction(
            func,
            *watches,
            shard_hint=shard_hint,
            value_from_callable=value_from_callable,
            watch_delay=watch_delay,
        )

    async def close(self):
        if self.__master:
            await self.__master.close()
        if self.__slave:
            await self.__slave.close()
        if self.__sentinel:
            self.__sentinel = None
        self.connected = False

    async def ping(self) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return await self.__master.ping()

    async def set(
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
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__master.set(
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

    async def get(self, name: KeyT) -> Any:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        value = await self.__slave.get(name)
        if value is None:
            return None
        return pickle.loads(value)

    async def incr(self, name: KeyT, amount: int = 1) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.incr(name, amount)

    async def decr(self, name: KeyT, amount: int = 1) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.decr(name, amount)

    async def append(self, name: KeyT, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.append(name, value)

    async def getrange(self, name: KeyT, start: int, end: int) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.getrange(name, start, end)

    async def setrange(self, name: KeyT, offset: int, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.setrange(name, offset, value)

    async def strlen(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.strlen(name)

    async def mget(self, keys: list[KeyT], *args: KeyT) -> list[Any]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(k) for k in keys] + [self.make_key(k) for k in args]
        values = await self.__slave.mget(all_keys)
        return [pickle.loads(v) if v is not None else None for v in values]

    async def mset(self, mapping: dict[KeyT, EncodableT]) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        prefixed_mapping = {self.make_key(k): pickle.dumps(v) for k, v in mapping.items()}
        return await self.__master.mset(prefixed_mapping)

    async def delete(self, *names: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        names_list = [self.make_key(name) for name in names]
        return await self.__master.delete(*names_list)

    async def delete_raw(self, *names: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return await self.__master.delete(*names)

    async def lpush(self, name: KeyT, *values: FieldT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_values = [pickle.dumps(v) for v in values]
        return await self.__master.lpush(name, *encoded_values)

    async def rpush(self, name: KeyT, *values: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_values = [pickle.dumps(v) for v in values]
        return await self.__master.rpush(name, *encoded_values)

    async def lpop(self, name: KeyT, count: Optional[int] = None) -> Any:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        value = await self.__master.lpop(name, count)
        if value is None:
            return None
        if isinstance(value, list):
            return [pickle.loads(v) for v in value]
        return pickle.loads(value)

    async def rpop(self, name: KeyT, count: Optional[int] = None) -> Any:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        value = await self.__master.rpop(name, count)
        if value is None:
            return None
        if isinstance(value, list):
            return [pickle.loads(v) for v in value]
        return pickle.loads(value)

    async def lrange(self, name: KeyT, start: int, end: int) -> list[Any]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        values = await self.__slave.lrange(name, start, end)
        return [pickle.loads(v) for v in values]

    async def llen(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.llen(name)

    async def lindex(self, name: KeyT, index: int) -> Any:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        value = await self.__slave.lindex(name, index)
        return pickle.loads(value) if value is not None else None

    async def lset(self, name: KeyT, index: int, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__master.lset(name, index, encoded_value)

    async def lrem(self, name: KeyT, count: int, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__master.lrem(name, count, encoded_value)

    async def ltrim(self, name: KeyT, start: int, end: int) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.ltrim(name, start, end)

    async def exists(self, *names: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        names_list = [self.make_key(name) for name in names]
        return await self.__slave.exists(*names_list)

    async def keys(self, pattern: PatternT = "*") -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        pattern = self.make_key(pattern)
        return await self.__slave.keys(pattern)

    async def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        mapping: Optional[dict] = None,
        items: Optional[list] = None,
    ) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.hset(name, key, value, mapping, items)  # type: ignore

    async def hget(self, name: str, key: str) -> Union[Awaitable[Optional[str]], Optional[str]]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hget(name, key)  # type: ignore

    async def hgetall(self, name: str) -> Union[Awaitable[dict], dict]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hgetall(name)  # type: ignore

    async def hdel(self, name: str, *keys: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.hdel(name, *keys)  # type: ignore

    async def hkeys(self, name: str) -> Union[Awaitable[list], list]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hkeys(name)  # type: ignore

    async def hvals(self, name: str) -> Union[Awaitable[list], list]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hvals(name)  # type: ignore

    async def hlen(self, name: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hlen(name)  # type: ignore

    async def hexists(self, name: str, key: str) -> Union[Awaitable[bool], bool]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hexists(name, key)  # type: ignore

    async def hincrby(self, name: str, key: str, amount: int = 1) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.hincrby(name, key, amount)  # type: ignore

    async def hincrbyfloat(
        self, name: str, key: str, amount: float = 1.0
    ) -> Union[Awaitable[float], float]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.hincrbyfloat(name, key, amount)  # type: ignore

    async def hmget(self, name: str, keys: list[str], *args: str) -> Union[Awaitable[list], list]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hmget(name, keys, *args)  # type: ignore

    async def hsetnx(self, name: str, key: str, value: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.hsetnx(name, key, value)  # type: ignore

    async def hstrlen(self, name: str, key: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hstrlen(name, key)  # type: ignore

    async def hrandfield(
        self, name: str, count: Optional[int] = None, withvalues: bool = False
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hrandfield(name, count, withvalues)

    async def hscan(
        self,
        name: str,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hscan(name, cursor, match, count, no_values)

    async def hscan_iter(
        self,
        name: str,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> AsyncIterator:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.hscan_iter(name, match, count, no_values)  # type: ignore

    async def flushdb(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return await self.__master.flushdb(asynchronous=asynchronous, **kwargs)

    async def flushall(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        return await self.__master.flushall(asynchronous=asynchronous, **kwargs)

    async def scan(
        self,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs: Any,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return await self.__slave.scan(cursor, match, count, _type, **kwargs)

    async def scan_iter(
        self,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs: Any,
    ) -> AsyncIterator:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        return await self.__slave.scan_iter(match, count, _type, **kwargs)  # type: ignore

    async def sadd(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.sadd(name, *values)  # type: ignore

    async def srem(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.srem(name, *values)  # type: ignore

    async def smembers(self, name: str) -> Union[Awaitable[Set], Set]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.smembers(name)  # type: ignore

    async def sismember(
        self, name: str, value: str
    ) -> Union[Awaitable[Union[Literal[0], Literal[1]]], Union[Literal[0], Literal[1]]]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.sismember(name, value)  # type: ignore

    async def smove(self, src: str, dst: str, value: str) -> Union[Awaitable[bool], bool]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        src = self.make_key(src)
        dst = self.make_key(dst)
        return await self.__master.smove(src, dst, value)  # type: ignore

    async def scard(self, name: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.scard(name)  # type: ignore

    async def sdiff(self, keys: list[str], *args: str) -> Union[Awaitable[Set], Set]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(k) for k in keys] + [self.make_key(k) for k in args]
        return await self.__slave.sdiff(all_keys)  # type: ignore

    async def sinter(self, keys: list[str], *args: str) -> Union[Awaitable[Set], Set]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(k) for k in keys] + [self.make_key(k) for k in args]
        return await self.__slave.sinter(all_keys)  # type: ignore

    async def sunion(self, keys: list[str], *args: str) -> Union[Awaitable[Set], Set]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        all_keys = [self.make_key(k) for k in keys] + [self.make_key(k) for k in args]
        return await self.__slave.sunion(all_keys)  # type: ignore

    async def spop(self, name: str, count: Optional[int] = None) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.spop(name, count)

    async def srandmember(self, name: str, number: Optional[int] = None) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.srandmember(name, number)

    async def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.sscan(name, cursor, match, count)

    async def sscan_iter(
        self,
        name: KeyT,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> AsyncIterator:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.sscan_iter(name, match, count)  # type: ignore

    async def zadd(
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
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_mapping = {k: pickle.dumps(v) for k, v in mapping.items()}
        return await self.__master.zadd(
            name=name,
            mapping=encoded_mapping,
            nx=nx,
            xx=xx,
            ch=ch,
            incr=incr,
            gt=gt,
            lt=lt,
        )

    async def zrem(self, name: str, *values: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_values = [pickle.dumps(v) for v in values]
        return await self.__master.zrem(name, *encoded_values)

    async def zrange(
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
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__slave.zrange(
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

    async def zrevrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__slave.zrevrange(
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

    async def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: Union[int, None] = None,
        num: Union[int, None] = None,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__slave.zrangebyscore(
            name, min, max, start, num, withscores, score_cast_func
        )
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    async def zrevrangebyscore(
        self,
        name: KeyT,
        max: ZScoreBoundT,
        min: ZScoreBoundT,
        start: Union[int, None] = None,
        num: Union[int, None] = None,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__slave.zrevrangebyscore(
            name, max, min, start, num, withscores, score_cast_func
        )
        if withscores:
            return [(pickle.loads(member), score) for member, score in result]
        else:
            return [pickle.loads(member) for member in result]

    async def zcard(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.zcard(name)

    async def zcount(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.zcount(name, min, max)

    async def zrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__slave.zrank(name, encoded_value, withscore)

    async def zrevrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__slave.zrevrank(name, encoded_value, withscore)

    async def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.zscan(
            name=name,
            cursor=cursor,
            match=match,
            count=count,
            score_cast_func=score_cast_func,
        )

    async def zscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> AsyncIterator:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.zscan_iter(
            name=name, match=match, count=count, score_cast_func=score_cast_func
        )  # type: ignore

    async def zremrangebyrank(self, name: KeyT, min: int, max: int) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.zremrangebyrank(name, min, max)

    async def zscore(self, name: KeyT, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__slave.zscore(name, encoded_value)

    async def zincrby(self, name: KeyT, amount: float, value: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        encoded_value = pickle.dumps(value)
        return await self.__master.zincrby(name, amount, encoded_value)

    async def zpopmin(self, name: KeyT, count: Optional[int] = None) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__master.zpopmin(name, count)
        if result:
            return [(pickle.loads(member), score) for member, score in result]
        return result

    async def zpopmax(self, name: KeyT, count: Optional[int] = None) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        result = await self.__master.zpopmax(name, count)
        if result:
            return [(pickle.loads(member), score) for member, score in result]
        return result

    async def zremrangebyscore(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.zremrangebyscore(name, min, max)

    async def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: Optional[int] = None,
        num: Optional[int] = None,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.zrangebylex(name, min, max, start, num)

    async def expire(self, name: KeyT, time: ExpiryT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.expire(name, time)

    async def expireat(self, name: KeyT, when: AbsExpiryT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.expireat(name, when)

    async def ttl(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.ttl(name)

    async def pttl(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.pttl(name)

    async def persist(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__master.persist(name)

    async def rename(self, src: KeyT, dst: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError("Master is not connected. Please check your connection settings.")
        src = self.make_key(src)
        dst = self.make_key(dst)
        return await self.__master.rename(src, dst)

    async def type(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError("Slave is not connected. Please check your connection settings.")
        name = self.make_key(name)
        return await self.__slave.type(name)
