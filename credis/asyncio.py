import contextlib
from redis.asyncio import Sentinel, Redis
from redis.asyncio.client import Pipeline
from credis.exceptions import InitError
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
from redis.typing import (
    KeyT,
    EncodableT,
    ResponseT,
    PatternT,
    ExpiryT,
    AbsExpiryT,
    FieldT,
    AnyKeyT,
    ZScoreBoundT,
)


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
        self.__sentinel = None
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
            raise ConnectionError(
                f"Failed to connect to Redis master/slave: {e}"
            ) from e

    async def write_pipeline_ctx(
        self, transaction: bool = True
    ) -> AsyncIterator[Pipeline]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        async with self.__master.pipeline(transaction) as pipe:
            yield pipe

    async def write_pipeline(self, transaction: bool = True) -> Pipeline:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        return await self.__master.pipeline(transaction)

    async def read_pipeline_ctx(self) -> AsyncIterator[Pipeline]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        async with self.__slave.pipeline() as pipe:
            yield pipe

    async def read_pipeline(self) -> Pipeline:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.set(
            name=name,
            value=value,
            ex=ex,
            px=px,
            nx=nx,
            xx=xx,
            keepttl=keepttl,
            get=get,
            exat=exat,
            pxat=pxat,
        )

    async def get(self, name: KeyT) -> Optional[ResponseT]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.get(name)

    async def delete(self, *names: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        names_list = [f"{self.__app_prefix}:{str(name)}" for name in names]
        return await self.__master.delete(*names_list)

    async def exists(self, *names: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        names_list = [f"{self.__app_prefix}:{str(name)}" for name in names]
        return await self.__slave.exists(*names_list)

    async def keys(self, pattern: PatternT = "*") -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        pattern = f"{self.__app_prefix}:{str(pattern)}"
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.hset(name, key, value, mapping, items)  # type: ignore

    async def hget(
        self, name: str, key: str
    ) -> Union[Awaitable[Optional[str]], Optional[str]]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hget(name, key)  # type: ignore

    async def hgetall(self, name: str) -> Union[Awaitable[dict], dict]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hgetall(name)  # type: ignore

    async def hdel(self, name: str, *keys: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.hdel(name, *keys)  # type: ignore

    async def hkeys(self, name: str) -> Union[Awaitable[list], list]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hkeys(name)  # type: ignore

    async def hvals(self, name: str) -> Union[Awaitable[list], list]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hvals(name)  # type: ignore

    async def hlen(self, name: str) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hlen(name)  # type: ignore

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.hscan_iter(name, match, count, no_values)  # type: ignore

    async def flushdb(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        return await self.__master.flushdb(asynchronous=asynchronous, **kwargs)

    async def flushall(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        return await self.__slave.scan_iter(match, count, _type, **kwargs)  # type: ignore

    async def sadd(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.sadd(name, *values)  # type: ignore

    async def srem(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.srem(name, *values)  # type: ignore

    async def smembers(self, name: str) -> Union[Awaitable[Set], Set]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.smembers(name)  # type: ignore

    async def sismember(
        self, name: str, value: str
    ) -> Union[Awaitable[Union[Literal[0], Literal[1]]], Union[Literal[0], Literal[1]]]:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.sismember(name, value)  # type: ignore

    async def smove(
        self, src: str, dst: str, value: str
    ) -> Union[Awaitable[bool], bool]:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        src = f"{self.__app_prefix}:{str(src)}"
        dst = f"{self.__app_prefix}:{str(dst)}"
        return await self.__master.smove(src, dst, value)  # type: ignore

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.zadd(
            name=name, mapping=mapping, nx=nx, xx=xx, ch=ch, incr=incr, gt=gt, lt=lt
        )

    async def zrem(self, name: str, *values: EncodableT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.zrem(name, *values)

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrange(
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrevrange(
            name=name,
            start=start,
            end=end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrangebyscore(
            name, min, max, start, num, withscores, score_cast_func
        )

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrevrangebyscore(
            name, max, min, start, num, withscores, score_cast_func
        )

    async def zcard(self, name: KeyT) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zcard(name)

    async def zcount(
        self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrank(name, value, withscore)

    async def zrevrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if not self.connected:
            await self.connect()
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zrevrank(name, value, withscore)

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__slave.zscan_iter(
            name=name, match=match, count=count, score_cast_func=score_cast_func
        )  # type: ignore

    async def zremrangebyrank(self, name: KeyT, min: int, max: int) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return await self.__master.zremrangebyrank(name, min, max)
