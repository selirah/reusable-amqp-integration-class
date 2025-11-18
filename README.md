# AMQP Integration & Streaming Message Queue

This repository provides Python classes for working with **AMQP (Advanced Message Queuing Protocol)** using the `py-amqp` library. It includes robust connection handling, background message streaming, thread-safe access, and automatic retries.

---

## Classes

### `AMQPIntegration`

Manages a single `py-amqp` connection (not thread-safe). Create one instance per thread or process.

**Features:**

- Automatically reconnects if the connection drops.
- Creates channels with exponential backoff and retries.
- Computes AMQP broker URI (supports environment-based virtual host suffix).
- Cleanly closes connections on object deletion.

**Usage:**

```python
from app.amqp_integration import AMQPIntegration

# Initialize the integration
amqp_integration = AMQPIntegration()

# Get a live connection
connection = amqp_integration.get_connection()

# Get a channel (with retries)
channel = amqp_integration.get_channel_with_retries()

# Get AMQP URI
uri = amqp_integration.compute_amqp_broker_uri()
print(uri)
```


# AMQPStreamingMessageQueue & MessageStream

This module provides a **thread-safe, AMQP-backed streaming message queue** and a **queue adapter** for consuming messages from AMQP with a local Python queue.

---

## Classes

### `AMQPStreamingMessageQueue`

A streaming message queue that uses a dedicated background thread to handle AMQP messages.

**Key Features:**

- Background thread for publishing, subscribing, and processing messages.
- Thread-safe channel management using locks.
- Durable AMQP exchange with proper message encoding (`JSON`, `str`, `bytes`).
- Automatic retries for message processing failures.
- Health checks for the background thread.
- Graceful shutdown of threads and channels.

**Usage Example:**

```python
from app.amqp_streaming_message_queue import AMQPStreamingMessageQueue

# Initialize the streaming queue
mq = AMQPStreamingMessageQueue()

# Publish a message
mq.publish_message(routing_key="my.routing.key", message={"hello": "world"})

# Subscribe to messages with a callback
def callback(payload):
    print("Received message:", payload)

mq.subscribe(routing_key="my.routing.key", callback=callback)

# Check if background thread is healthy
print("Thread healthy:", mq.is_background_thread_healthy())

# Shutdown queue when done
mq.shutdown()
```