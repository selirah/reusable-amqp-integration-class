import os
import asyncio
import json
import time
import random
import socket
import threading
import queue
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, Union, Sequence

import amqp
from amqp.exceptions import (
    ConnectionError as AmqpConnectionError,
    ChannelError as AmqpChannelError,
    RecoverableConnectionError as AmqpRecoverableConnectionError,
)

from app.utils.logger import get_logger
from app.utils.random import generate_random_string
from app.amqp_integration import AMQPIntegration

@dataclass(frozen=True, slots=True)
class ThreadMessage:
    message_type: str
    data: dict
    retries_left: int
    response_queue: Optional[queue.Queue] = None


class AMQPStreamingMessageQueue:
    """
    AMQP-backed streaming message queue with a dedicated background thread.

    Key improvements:
      - Safe locking for all channel map reads/writes.
      - Correct timeout handling for amqp.Connection.drain_events.
      - Durable exchange (no auto-delete by default).
      - Proper message encoding (bytes/str/json) with content_type.
      - Retry without tearing down the thread loop on per-message errors.
      - Health-check and clean shutdown.
    """

    logger = get_logger("AMQPStreamingMessageQueue")

    inactive_channel_timeout = 305
    message_thread_loop_busy_wait_time = 0.01
    exchange_name = "message-events"
    thread_message_retry_limit = 10
    health_check_timeout_seconds = 3.0

    _amqp_integration: Optional[AMQPIntegration]
    _channels_by_routing_key: dict
    _channels_lock: threading.Lock
    _last_use_time: dict
    thread_queue_in: queue.Queue
    connection: Optional[amqp.Connection]
    running: bool

    def __init__(self):
        self._channels_by_routing_key = {}
        self._channels_lock = threading.Lock()
        self._last_use_time = {}
        self.thread_queue_in = queue.Queue()
        self.connection = None
        self.running = False
        self._amqp_integration = None
        self.pid_at_initialization = os.getpid()
        self.message_thread: Optional[threading.Thread] = None
        self.restart_thread()

    def __del__(self):
        self.shutdown()

    # ---------------- public API ----------------

    def publish_message(self, routing_key, message):
        self._send_message_to_thread("publish", routing_key=routing_key, message=message)

    def subscribe_to_message_stream_for_routing_key(self, routing_key):
        return MessageStream(self, routing_key)

    def subscribe(self, routing_key, callback):
        return self._send_message_to_thread_and_get_response(
            "subscribe", routing_key=routing_key, callback=callback
        )

    def cancel(self, routing_key, consumer):
        return self._send_message_to_thread(
            "cancel", routing_key=routing_key, consumer_id=consumer
        )

    def is_background_thread_healthy(self) -> bool:
        try:
            response = self._send_message_to_thread_and_get_response(
                message_type="health_check",
                timeout=self.health_check_timeout_seconds,
            )
            return response == "healthy"
        except queue.Empty:
            return False

    def shutdown(self):
        if self.message_thread is not None:
            self.logger.info("Shutting down message events thread")
            if self.running and self.message_thread.is_alive():
                self._send_message_to_thread("shutdown")
                self.running = False
                self.message_thread.join()
        self.message_thread = None

    # ---------------- thread lifecycle ----------------

    def ensure_thread_is_running(self):
        if self.message_thread is None:
            self.restart_thread()
        elif not self.message_thread.is_alive() or not self.running:
            self.restart_thread()

    def restart_thread(self):
        self.shutdown()
        self.logger.info("Starting a new message events thread in the background")
        self.message_thread = threading.Thread(
            target=self._message_events_thread,
            name="amqp_streaming_message_events_thread",
            daemon=True,
        )
        self.message_thread.start()

    # ---------------- inter-thread messaging ----------------

    def _check_is_correct_process_pid(self):
        if os.getpid() != self.pid_at_initialization:
            raise AssertionError(
                "AMQPStreamingMessageQueue used from a different process than it was created in."
            )

    def _send_message_to_thread(self, message_type, **data):
        self._check_is_correct_process_pid()
        self.ensure_thread_is_running()
        self.thread_queue_in.put(
            ThreadMessage(
                message_type=message_type,
                data=data,
                response_queue=None,
                retries_left=self.thread_message_retry_limit,
            )
        )

    def _send_message_to_thread_and_get_response(self, message_type, timeout: float = 3.0, **data):
        self._check_is_correct_process_pid()
        self.ensure_thread_is_running()
        response_queue = queue.Queue()
        self.thread_queue_in.put(
            ThreadMessage(
                message_type=message_type,
                data=data,
                response_queue=response_queue,
                retries_left=self.thread_message_retry_limit,
            )
        )
        return response_queue.get(block=True, timeout=timeout)

    # ---------------- AMQP connection/channel helpers ----------------

    def _initialize_amqp_connection(self) -> amqp.Connection:
        self._amqp_integration = AMQPIntegration()
        self.connection = self._amqp_integration.get_connection()

        # Sanity test and exchange declaration
        ch = self.connection.channel()
        ch.queue_declare(auto_delete=True, exclusive=True)

        # Durable exchange; let queues be auto-delete if you want ephemeral subs
        ch.exchange_declare(
            exchange=self.exchange_name,
            type="direct",
            durable=True,
            auto_delete=False,
        )

        self.logger.info("AMQP connected successfully")
        return self.connection

    def _close_connection(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except (OSError, AmqpConnectionError, AmqpChannelError):
                # best-effort close
                pass
            self.connection = None

        with self._channels_lock:
            self._channels_by_routing_key = {}
            self._last_use_time = {}

    @staticmethod
    def _get_routing_key_string(routing_key: Union[str, Sequence[str]]) -> str:
        if isinstance(routing_key, (list, tuple)):
            return ".".join(routing_key)
        if isinstance(routing_key, str):
            return routing_key
        raise ValueError(f"Invalid routing key type {type(routing_key)}: {routing_key!r}")

    # ---------------- background thread ----------------

    def _message_events_thread(self):
        self.running = True
        restarts = 0
        max_restarts = 100
        fail_count = 0

        while restarts < max_restarts and self.running:
            try:
                self.logger.info("Starting the message events thread")
                time.sleep(random.uniform(0.0, 1.0))
                self._initialize_amqp_connection()
                fail_count = 0

                while self.running:
                    self._process_messages_from_amqp()
                    self._process_incoming_thread_messages()
                    self._close_inactive_channels()
                    time.sleep(self.message_thread_loop_busy_wait_time)

                self._force_close_all_channels()

            except (AmqpConnectionError, AmqpRecoverableConnectionError, AmqpChannelError, OSError, socket.error):
                self.logger.error("Error in message events thread", exc_info=True)
                # exponential backoff
                time.sleep(self.message_thread_loop_busy_wait_time * (2 ** min(fail_count, 10)))
                restarts += 1
                fail_count += 1
            finally:
                self._close_connection()

        self.logger.error("Message events thread has exited after %d restarts.", restarts)
        self.running = False

    def _process_messages_from_amqp(self):
        if self.connection is None:
            return
        try:
            self.connection.drain_events(timeout=self.message_thread_loop_busy_wait_time)
        except socket.timeout:
            # normal idle path
            pass

    def _process_incoming_thread_messages(self):
        while True:
            try:
                thread_message: ThreadMessage = self.thread_queue_in.get_nowait()
            except queue.Empty:
                break

            response = None
            try:
                t = thread_message.message_type
                if t == "new_channel":
                    response = self._handle_new_channel(thread_message)
                elif t == "shutdown":
                    self.running = False
                    break
                elif t == "publish":
                    response = self._handle_publish(thread_message)
                elif t == "subscribe":
                    response = self._handle_subscribe(thread_message)
                elif t == "health_check":
                    response = "healthy"
                elif t == "cancel":
                    response = self._handle_cancel(thread_message)

                if thread_message.response_queue is not None:
                    thread_message.response_queue.put(response)

            except (AmqpConnectionError, AmqpRecoverableConnectionError, AmqpChannelError, OSError, socket.error):
                self.logger.error("Error processing thread message %s", thread_message.message_type, exc_info=True)
                # Retry the message if allowed; do not tear down the whole loop.
                if thread_message.retries_left > 0:
                    self.thread_queue_in.put(
                        ThreadMessage(
                            message_type=thread_message.message_type,
                            data=thread_message.data,
                            response_queue=thread_message.response_queue,
                            retries_left=thread_message.retries_left - 1,
                        )
                    )

    # ---------------- message handlers (run in background thread) ----------------

    def _handle_new_channel(self, message: ThreadMessage):
        routing_key = message.data["routing_key"]
        with self._channels_lock:
            ch = self._channels_by_routing_key.get(routing_key)
            if ch is None:
                ch = self._create_new_channel_for_routing_key_no_lock(routing_key)
            self._last_use_time[routing_key] = datetime.now(tz=timezone.utc)
            return ch

    def _handle_publish(self, message: ThreadMessage):
        routing_key = message.data["routing_key"]
        payload = message.data["message"]

        with self._channels_lock:
            channel = self._channels_by_routing_key.get(routing_key)
            if channel is None:
                channel = self._create_new_channel_for_routing_key_no_lock(routing_key)
            self._last_use_time[routing_key] = datetime.now(tz=timezone.utc)

        # Encode message body safely
        if isinstance(payload, (bytes, bytearray)):
            body = bytes(payload)
            content_type = "application/octet-stream"
        elif isinstance(payload, str):
            body = payload.encode("utf-8")
            content_type = "text/plain; charset=utf-8"
        else:
            body = json.dumps(payload).encode("utf-8")
            content_type = "application/json"

        msg = amqp.Message(body, content_type=content_type, delivery_mode=1)
        channel.basic_publish(
            msg,
            exchange=self.exchange_name,
            routing_key=self._get_routing_key_string(routing_key),
            mandatory=False,
        )
        return channel

    def _handle_cancel(self, message: ThreadMessage):
        routing_key = message.data["routing_key"]
        consumer_id = message.data["consumer_id"]

        with self._channels_lock:
            ch = self._channels_by_routing_key.get(routing_key)
        if ch is None:
            return {"consumer_id": consumer_id}  # already gone / no-op

        ch.basic_cancel(consumer_id)
        return {"consumer_id": consumer_id}

    def _handle_subscribe(self, thread_message: ThreadMessage) -> Tuple[str, str]:
        routing_key = thread_message.data["routing_key"]
        callback = thread_message.data["callback"]

        with self._channels_lock:
            channel = self._channels_by_routing_key.get(routing_key)
            if channel is None:
                channel = self._create_new_channel_for_routing_key_no_lock(routing_key)
            self._last_use_time[routing_key] = datetime.now(tz=timezone.utc)

        def on_message(message):
            try:
                with self._channels_lock:
                    self._last_use_time[routing_key] = datetime.now(tz=timezone.utc)

                body = message.body
                payload = self._decode_message_body(body)

                callback(payload)

            except Exception as e:
                # Catch only subscriber callback errors
                self.logger.error(f"Subscriber callback raised: {e}", exc_info=True)

        random_nonce = generate_random_string(8) + str(time.time())
        queue_name = f"streaming_message_queue_consumer_{routing_key}_{random_nonce}"

        channel.queue_declare(queue=queue_name, exclusive=True, auto_delete=True)
        channel.queue_bind(
            queue=queue_name,
            exchange=self.exchange_name,
            routing_key=self._get_routing_key_string(routing_key),
        )
        consumer_tag = channel.basic_consume(
            queue=queue_name, no_ack=True, no_local=False, callback=on_message
        )
        return routing_key, consumer_tag

    # ---------------- channel housekeeping ----------------

    def _create_new_channel_for_routing_key_no_lock(self, routing_key):
        self.logger.debug("Opening a channel for routing_key %s", routing_key)
        ch = self.connection.channel()
        self._channels_by_routing_key[routing_key] = ch
        self._last_use_time[routing_key] = datetime.now(tz=timezone.utc)
        return ch

    def _force_close_all_channels(self):
        with self._channels_lock:
            for rk, ch in list(self._channels_by_routing_key.items()):
                self.logger.info("Closing channel for routing %s", rk)
                try:
                    ch.close()
                except (OSError, AmqpConnectionError, AmqpChannelError):
                    pass
            self._channels_by_routing_key = {}
            self._last_use_time = {}

    def _close_inactive_channels(self):
        now = datetime.now(tz=timezone.utc)
        to_delete = []

        with self._channels_lock:
            for rk, last_use in list(self._last_use_time.items()):
                if last_use is None:
                    continue
                if (now - last_use).total_seconds() > self.inactive_channel_timeout:
                    to_delete.append(rk)

            for rk in to_delete:
                self.logger.debug("Closing inactive channel for routing %s", rk)
                ch = self._channels_by_routing_key.pop(rk, None)
                self._last_use_time.pop(rk, None)
                if ch is not None:
                    try:
                        ch.close()
                    except (OSError, AmqpConnectionError, AmqpChannelError):
                        pass

    @staticmethod
    def _decode_message_body(body):
        """Try to decode an AMQP message body to JSON, str, or bytes."""
        if isinstance(body, (bytes, bytearray)):
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return bytes(body)
        if isinstance(body, str):
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return body
        return body


class MessageStream:
    """
    Simple adapter that pipes AMQP consumer callbacks into a local Queue.
    """

    def __init__(self, mq: AMQPStreamingMessageQueue, routing_key):
        self.mq = mq
        self.routing_key = routing_key
        self.consumer: Optional[str] = None
        self.queue: "queue.Queue[object]" = queue.Queue()

    def __del__(self):
        self.shutdown()

    def __enter__(self):
        self.start_consuming()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    async def __aenter__(self):
        self.start_consuming()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.shutdown()

    def __iter__(self):
        return self

    def get(self, *args, **kwargs):
        return self.queue.get(*args, **kwargs)

    async def get_async(self, timeout: float | None = None):
        # queue.Queue.get is blocking; offload to a thread
        return await asyncio.to_thread(self.queue.get, True, timeout)

    def on_message(self, msg):
        self.queue.put(msg)

    def start_consuming(self):
        response = self.mq.subscribe(routing_key=self.routing_key, callback=self.on_message)
        assert response[0] == self.routing_key
        self.consumer = response[1]

    def shutdown(self):
        if self.consumer is not None:
            self.mq.cancel(routing_key=self.routing_key, consumer=self.consumer)
            self.consumer = None