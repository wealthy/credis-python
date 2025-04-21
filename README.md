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
