import os
import time
import amqp
import socket
from urllib.parse import quote

from amqp import RecoverableConnectionError, ChannelError

from app.utils.logger import get_logger
from app.core.config import settings


class AMQPIntegration:
    """
    Manages a single py-amqp connection (not thread-safe).
    Create one instance per thread/process that uses it.
    """

    def __init__(self):
        self._logger = get_logger("amqp_integration")
        self.connection: amqp.Connection | None = None
        self.config = settings

    def __del__(self):
        try:
            if self.connection is not None:
                self.connection.close()
        except (amqp.exceptions.ConnectionError, OSError, socket.error):
            # best effort; avoid destructor exceptions
            pass

    # ---------- connection management ----------

    def get_connection(self) -> amqp.Connection:
        """
        Return a live amqp.Connection, (re)connecting as needed.
        """
        if self.connection is None or not getattr(self.connection, "connected", False):
            self.connection = self._create_amqp_connection()
        return self.connection

    def _create_amqp_connection(self) -> amqp.Connection:
        host = self.config.AMQP_HOST
        port = self.config.AMQP_PORT
        username = self.config.AMQP_USERNAME
        password = self.config.AMQP_PASSWORD
        vhost = self._compute_vhost_with_suffix(self.config.AMQP_VHOST)

        # Optional dial settings (tune to your infra)
        heartbeat = getattr(self.config, "heartbeat", 60)            # seconds; 0 = disabled
        connect_timeout = getattr(self.config, "connect_timeout", 10)  # seconds

        self._logger.info(
            "Connecting to AMQP host=%s:%s vhost=%s user=%s",
            host, port, vhost, username
        )

        conn = amqp.Connection(
            host=f"{host}:{port}",
            userid=username,
            password=password,
            virtual_host=vhost,            # <-- correct kwarg
            heartbeat=heartbeat,
            connect_timeout=connect_timeout,
            # ssl=...  # if you need TLS, wire via settings here
        )
        conn.connect()
        return conn

    def get_channel_with_retries(self) -> amqp.Channel:
        """
        Obtain a channel with exponential backoff; tear down broken connections.
        """
        backoff = 0.5
        for attempt in range(10):
            try:
                return self.get_connection().channel()
            except (socket.error, OSError, amqp.exceptions.ConnectionError) as ex:
                self._logger.error("Error getting AMQP channel (attempt %d): %s", attempt + 1, ex, exc_info=True)
                # Try to close and drop the connection before retrying
                try:
                    if self.connection is not None:
                        self.connection.close()
                except (OSError, socket.error, ConnectionError, RecoverableConnectionError, ChannelError) as ex:
                    self._logger.error("Error getting AMQP channel (attempt %d): %s", attempt + 1, ex, exc_info=True)
                self.connection = None
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
        raise RuntimeError("Unable to get AMQP channel after 10 retries.")

    # ---------- helpers ----------

    def compute_amqp_broker_uri(self) -> str:
        """
        amqp://user:pass@host:port/vhost  (URL-encoded)
        """
        host = self.config.AMQP_HOST
        port = self.config.AMQP_PORT
        username = quote(str(self.config.AMQP_USERNAME), safe="")
        password = quote(str(self.config.AMQP_PASSWORD), safe="")
        vhost = quote(self._compute_vhost_with_suffix(self.config.AMQP_VHOST), safe="")
        return f"amqp://{username}:{password}@{host}:{port}/{vhost}"

    @staticmethod
    def _compute_vhost_with_suffix(vhost: str) -> str:
        suffix = os.environ.get("AMQP_VIRTUAL_HOST_SUFFIX")
        return f"{vhost}{suffix}" if suffix else vhost