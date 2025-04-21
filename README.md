# Credis Python

A Python client library for Redis with Sentinel support with command auto routing to master/slave replicas.

## Overview

Credis-Python is a Redis client library that provides a simplified interface for working with Redis Sentinel for high availability Redis deployments. It wraps the standard Redis-py library and provides both synchronous and asynchronous APIs.

## Features

- Built-in Redis Sentinel support for high availability
- Master/Slave separation for read/write operations
- Application prefix for key namespace isolation
- Both synchronous and asynchronous APIs
- Comprehensive Redis commands support
- Pipeline support for batching operations

## Installation

```bash
pip install git+https://github.com/wealthy/credis-python.git
```

```bash
uv add git+https://github.com/wealthy/credis-python.git
```

## Usage

### Synchronous Client

```python
from credis import Client

# Initialize the client
client = Client(
    host="127.0.0.1",         # Redis Sentinel host
    port="26379",             # Redis Sentinel port
    app_prefix="myapp",       # Application prefix for namespace isolation
    password="password",      # Optional: Redis password
    socket_timeout=0.5,       # Optional: Socket timeout in seconds
    masterset_name="mymaster" # Optional: Master set name in Sentinel
)

# Basic operations
client.set("key", "value")
value = client.get("key")
print(value)  # Output: value

# Delete a key
client.delete("key")

# Check if a key exists
exists = client.exists("key")
print(exists)  # Output: 0 (key doesn't exist)

# Working with hashes
client.hset("myhash", "field1", "value1")
client.hset("myhash", mapping={"field2": "value2", "field3": "value3"})
value = client.hget("myhash", "field1")
print(value)  # Output: value1
all_values = client.hgetall("myhash")
print(all_values)  # Output: {b'field1': b'value1', b'field2': b'value2', b'field3': b'value3'}

# Working with sets
client.sadd("myset", "member1", "member2", "member3")
members = client.smembers("myset")
print(members)  # Output: {b'member1', b'member2', b'member3'}
is_member = client.sismember("myset", "member1")
print(is_member)  # Output: 1 (member exists)

# Working with sorted sets
client.zadd("myzset", {"member1": 1.0, "member2": 2.0, "member3": 3.0})
members = client.zrange("myzset", 0, -1, withscores=True)
print(members)  # Output: [(b'member1', 1.0), (b'member2', 2.0), (b'member3', 3.0)]

# Using pipeline for batching operations
pipe = client.write_pipeline()
pipe.set("key1", "value1")
pipe.set("key2", "value2")
pipe.get("key1")
pipe.get("key2")
results = pipe.execute()
print(results)  # Output: [True, True, b'value1', b'value2']

# Close the connection when done
client.close()
```

### Asynchronous Client

```python
import asyncio
from credis.asyncio import AsyncClient

async def main():
    # Initialize the async client
    client = AsyncClient(
        host="127.0.0.1",         # Redis Sentinel host
        port="26379",             # Redis Sentinel port
        app_prefix="myapp",       # Application prefix for namespace isolation
        password="password",      # Optional: Redis password
        socket_timeout=0.5,       # Optional: Socket timeout in seconds
        masterset_name="mymaster" # Optional: Master set name in Sentinel
    )

    # Connect to Redis.
    await client.connect() # This is optional

    # Basic operations
    await client.set("key", "value")
    value = await client.get("key")
    print(value)  # Output: value

    # Delete a key
    await client.delete("key")

    # Check if a key exists
    exists = await client.exists("key")
    print(exists)  # Output: 0 (key doesn't exist)

    # Working with hashes
    await client.hset("myhash", "field1", "value1")
    await client.hset("myhash", mapping={"field2": "value2", "field3": "value3"})
    value = await client.hget("myhash", "field1")
    print(value)  # Output: value1
    all_values = await client.hgetall("myhash")
    print(all_values)  # Output: {b'field1': b'value1', b'field2': b'value2', b'field3': b'value3'}

    # Using pipeline for batching operations
    pipe = client.write_pipeline()
    pipe.set("key1", "value1")
    pipe.set("key2", "value2")
    pipe.get("key1")
    pipe.get("key2")
    results = await pipe.execute()
    print(results)  # Output: [True, True, b'value1', b'value2']

    # Close the connection when done
    await client.close()

# Run the async example
asyncio.run(main())
```

## Key Features

### Application Prefix

All keys are automatically prefixed with your application prefix to avoid key collisions in a shared Redis instance:

```python
client = Client(host="127.0.0.1", port="26379", app_prefix="myapp")
client.set("user:1", "John")  # Actually sets "myapp:user:1"
```

### Read/Write Separation

Credis automatically routes write operations to the master and read operations to the slave:

- Write operations (set, delete, hset, etc.) go to the master
- Read operations (get, exists, hget, etc.) go to the slave

### Pipeline Support

Credis provides separate pipelines for read and write operations:

```python
# Write pipeline (to the master)
write_pipe = client.write_pipeline()
write_pipe.set("key1", "value1")
write_pipe.set("key2", "value2")
write_results = write_pipe.execute()

# Read pipeline (from the slave)
read_pipe = client.read_pipeline()
read_pipe.get("key1")
read_pipe.get("key2")
read_results = read_pipe.execute()
```

## Error Handling

Credis provides several exception types for different error cases:

```python
from credis.exceptions import ConnectionError, SentinelError, InitError

try:
    client = Client(host="127.0.0.1", port="26379", app_prefix="myapp")
    client.set("key", "value")
except SentinelError as e:
    print(f"Error connecting to Redis Sentinel: {e}")
except ConnectionError as e:
    print(f"Error connecting to Redis: {e}")
except InitError as e:
    print(f"Initialization error: {e}")
```

## FastAPI Integration with Dependency Injection

Credis can be easily integrated with FastAPI using its dependency injection system. The context manager methods (`write_pipeline_ctx` and `read_pipeline_ctx`) can be used directly with FastAPI's `Depends()`:

```python
from fastapi import FastAPI, Depends
from credis.asyncio import AsyncClient
from redis.asyncio.client import Pipeline

app = FastAPI()

# Create a Redis client
redis_client = AsyncClient(
    host="127.0.0.1",
    port="26379",
    app_prefix="myapp",
    password="password",
    socket_timeout=0.5,
    masterset_name="mymaster"
)

# Example endpoint using write_pipeline_ctx directly with dependency injection
@app.post("/users/{user_id}")
async def create_user(
    user_id: str,
    name: str,
    pipe: Pipeline = Depends(redis_client.write_pipeline_ctx)
):
    pipe.hset(f"user:{user_id}", mapping={"name": name, "created_at": "2025-04-21"})
    pipe.sadd("users", user_id)
    result = await pipe.execute()
    return {"success": True, "results": result}

# Example endpoint using read_pipeline_ctx directly with dependency injection
@app.get("/users/{user_id}")
async def get_user(
    user_id: str,
    pipe: Pipeline = Depends(redis_client.read_pipeline_ctx)
):
    pipe.hgetall(f"user:{user_id}")
    pipe.sismember("users", user_id)
    result = await pipe.execute()
    if not result[1]:
        return {"error": "User not found"}
    return {"user_id": user_id, "data": result[0]}

# Example combining both pipelines in the same endpoint
@app.put("/users/{user_id}")
async def update_user(
    user_id: str,
    name: str,
    read_pipe: Pipeline = Depends(redis_client.read_pipeline_ctx),
    write_pipe: Pipeline = Depends(redis_client.write_pipeline_ctx)
):
    # First check if user exists
    read_pipe.sismember("users", user_id)
    exists = (await read_pipe.execute())[0]

    if not exists:
        return {"error": "User not found"}

    # Then update user data
    write_pipe.hset(f"user:{user_id}", "name", name)
    write_pipe.hset(f"user:{user_id}", "updated_at", "2025-04-21")
    await write_pipe.execute()

    return {"success": True, "user_id": user_id}
```

This approach offers several benefits:

- Direct usage of the context manager methods with FastAPI's dependency system
- Automatic connection management and cleanup
- Proper context management of pipelines
- Clear separation of read and write operations
- Efficient batching of Redis commands

### Using Without Dependency Injection

You can also use Credis normally in FastAPI without dependency injection. This gives you more flexibility in how you manage the client lifecycle:

```python
from fastapi import FastAPI
from credis.asyncio import AsyncClient

app = FastAPI()

# Create a Redis client
redis_client = AsyncClient(
    host="127.0.0.1",
    port="26379",
    app_prefix="myapp",
    password="password"
)

# Example using pipeline context managers directly
@app.post("/users/{user_id}/with-pipeline")
async def create_user_with_pipeline(user_id: str, name: str):
    # Use pipeline context manager directly
    async with redis_client.write_pipeline_ctx() as pipe:
        pipe.hset(f"user:{user_id}", mapping={"name": name, "created_at": "2025-04-21"})
        pipe.sadd("users", user_id)
        result = await pipe.execute()

    return {"success": True, "results": result}

@app.get("/users/{user_id}/with-pipeline")
async def get_user_with_pipeline(user_id: str):
    # Use pipeline context manager directly
    async with redis_client.read_pipeline_ctx() as pipe:
        pipe.hgetall(f"user:{user_id}")
        pipe.sismember("users", user_id)
        result = await pipe.execute()

    if not result[1]:
        return {"error": "User not found"}
    return {"user_id": user_id, "data": result[0]}

# Examples using simple async/await without pipelines
@app.post("/users/{user_id}")
async def create_user(user_id: str, name: str):
    # Simple operations using async/await directly
    await redis_client.hset(
        f"user:{user_id}",
        mapping={"name": name, "created_at": "2025-04-21"}
    )
    await redis_client.sadd("users", user_id)
    return {"success": True}

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # Perform simple operations using async/await directly
    exists = await redis_client.sismember("users", user_id)

    if not exists:
        return {"error": "User not found"}

    user_data = await redis_client.hgetall(f"user:{user_id}")
    return {"user_id": user_id, "data": user_data}

@app.put("/users/{user_id}")
async def update_user(user_id: str, name: str):
    # Check if user exists first
    exists = await redis_client.sismember("users", user_id)

    if not exists:
        return {"error": "User not found"}

    # Update user data
    await redis_client.hset(f"user:{user_id}", "name", name)
    await redis_client.hset(f"user:{user_id}", "updated_at", "2025-04-21")

    return {"success": True, "user_id": user_id}

@app.delete("/users/{user_id}")
async def delete_user(user_id: str):
    # Check if user exists first
    exists = await redis_client.sismember("users", user_id)

    if not exists:
        return {"error": "User not found"}

    # Delete user data and remove from users set
    await redis_client.delete(f"user:{user_id}")
    await redis_client.srem("users", user_id)

    return {"success": True, "message": f"User {user_id} deleted"}
```

This approach is useful when:

1. You want more control over when the pipeline is created and executed
2. You need to use the Redis client for various purposes within a single endpoint
3. You prefer simpler code without pipelines for basic operations
4. You need to perform conditional operations based on intermediate results
