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
from redis import Sentinel
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
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Redis master/slave for {self.__masterset_name}: {str(e)}"
            ) from e

    def pipeline(self, transaction: bool = True) -> Pipeline:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        return self.__master.pipeline(transaction)

    def close(self):
        if self.__master:
            self.__master.close()
        if self.__slave:
            self.__slave.close()
        if self.__sentinel:
            self.__sentinel = None

    def ping(self) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.set(
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

    def get(self, name: KeyT) -> Optional[ResponseT]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.get(name)

    def delete(self, *names: KeyT) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        names_list = [f"{self.__app_prefix}:{str(name)}" for name in names]
        return self.__master.delete(*names_list)

    def exists(self, *names: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        names_list = [f"{self.__app_prefix}:{str(name)}" for name in names]
        return self.__slave.exists(*names_list)

    def keys(self, pattern: PatternT = "*") -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        pattern = f"{self.__app_prefix}:{str(pattern)}"
        return self.__slave.keys(pattern)

    def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        mapping: Optional[dict] = None,
        items: Optional[list] = None,
    ) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.hset(name, key, value, mapping, items)

    def hget(
        self, name: str, key: str
    ) -> Union[Awaitable[Optional[str]], Optional[str]]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hget(name, key)

    def hgetall(self, name: str) -> Union[Awaitable[dict], dict]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hgetall(name)

    def hdel(self, name: str, *keys: str) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.hdel(name, *keys)

    def hkeys(self, name: str) -> Union[Awaitable[list], list]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hkeys(name)

    def hvals(self, name: str) -> Union[Awaitable[list], list]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hvals(name)

    def hlen(self, name: str) -> Union[Awaitable[int], int]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hlen(name)

    def hscan(
        self,
        name: str,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hscan(name, cursor, match, count, no_values)

    def hscan_iter(
        self,
        name: str,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        no_values: Optional[bool] = None,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.hscan_iter(name, match, count, no_values)

    def flushdb(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        return self.__master.flushdb(asynchronous=asynchronous, **kwargs)

    def flushall(self, asynchronous: bool = False, **kwargs: Any) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        return self.__slave.scan(cursor, match, count, _type, **kwargs)

    def scan_iter(
        self,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        return self.__slave.scan_iter(match, count, _type, **kwargs)

    def sadd(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.sadd(name, *values)

    def srem(self, name: str, *values: FieldT) -> Union[Awaitable[int], int]:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.srem(name, *values)

    def smembers(self, name: str) -> Union[Awaitable[Set], Set]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.smembers(name)

    def sismember(
        self, name: str, value: str
    ) -> Union[Awaitable[Union[Literal[0], Literal[1]]], Union[Literal[0], Literal[1]]]:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.sismember(name, value)

    def smove(self, src: str, dst: str, value: str) -> Union[Awaitable[bool], bool]:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        src = f"{self.__app_prefix}:{str(src)}"
        dst = f"{self.__app_prefix}:{str(dst)}"
        return self.__master.smove(src, dst, value)

    def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.sscan(name, cursor, match, count)

    def sscan_iter(
        self,
        name: KeyT,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.sscan_iter(name, match, count)

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
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.zadd(
            name=name, mapping=mapping, nx=nx, xx=xx, ch=ch, incr=incr, gt=gt, lt=lt
        )

    def zrem(self, name: str, *values: EncodableT) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.zrem(name, *values)

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrange(
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

    def zrevrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrevrange(
            name=name,
            start=start,
            end=end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrangebyscore(
            name, min, max, start, num, withscores, score_cast_func
        )

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
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrevrangebyscore(
            name, max, min, start, num, withscores, score_cast_func
        )

    def zcard(self, name: KeyT) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zcard(name)

    def zcount(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zcount(name, min, max)

    def zrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrank(name, value, withscore)

    def zrevrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zrevrank(name, value, withscore)

    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> ResponseT:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zscan(
            name=name,
            cursor=cursor,
            match=match,
            count=count,
            score_cast_func=score_cast_func,
        )

    def zscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> Iterator:
        if self.__slave is None:
            raise InitError(
                "Slave is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__slave.zscan_iter(
            name=name, match=match, count=count, score_cast_func=score_cast_func
        )

    def zremrangebyrank(self, name: KeyT, min: int, max: int) -> ResponseT:
        if self.__master is None:
            raise InitError(
                "Master is not connected. Please check your connection settings."
            )
        name = f"{self.__app_prefix}:{str(name)}"
        return self.__master.zremrangebyrank(name, min, max)
