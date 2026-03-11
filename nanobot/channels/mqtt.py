"""MQTT channel for IoT device and local process communication.

Allows any MQTT client (sensors, detectors, scripts) to exchange messages
with the nanobot agent via an MQTT broker.

Protocol: JSON payloads on configurable topics.

Client → nanobot (subscribe_topic):
    {"sender_id": "sensor_01", "chat_id": "alerts", "content": "temperature=35", "metadata": {...}}

Nanobot → client (publish_topic):
    {"chat_id": "alerts", "content": "已收到，正在处理..."}
"""

import json
from typing import Any

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel


class MqttChannel(BaseChannel):
    """MQTT channel using aiomqtt."""

    name: str = "mqtt"

    def __init__(self, config: Any, bus: MessageBus):
        super().__init__(config, bus)
        self._client = None  # aiomqtt.Client, set during start()

    async def start(self) -> None:
        try:
            import aiomqtt
        except ImportError as exc:
            raise ImportError(
                "aiomqtt is required for the MQTT channel. "
                "Install it with: pip install aiomqtt"
            ) from exc

        kwargs: dict[str, Any] = {
            "hostname": self.config.broker_host,
            "port": self.config.broker_port,
            "identifier": self.config.client_id,
        }
        if self.config.username:
            kwargs["username"] = self.config.username
        if self.config.password:
            kwargs["password"] = self.config.password

        self._running = True
        logger.info(
            "MQTT channel connecting to {}:{} (topic={})",
            self.config.broker_host,
            self.config.broker_port,
            self.config.subscribe_topic,
        )

        async with aiomqtt.Client(**kwargs) as client:
            self._client = client
            await client.subscribe(self.config.subscribe_topic, qos=self.config.qos)
            logger.info("MQTT channel subscribed to {}", self.config.subscribe_topic)

            async for message in client.messages:
                if not self._running:
                    break
                await self._process_message(message)

        self._client = None
        logger.info("MQTT channel stopped")

    async def stop(self) -> None:
        self._running = False
        # Setting _running = False causes the message loop to exit on next iteration.
        # aiomqtt will clean up the connection when the context manager exits.
        logger.info("MQTT channel stopping...")

    async def send(self, msg: OutboundMessage) -> None:
        """Publish nanobot reply to the MQTT broker."""
        if self._client is None:
            logger.debug("MQTT: not connected, dropping reply for chat_id={}", msg.chat_id)
            return

        try:
            payload = json.dumps(
                {"chat_id": msg.chat_id, "content": msg.content},
                ensure_ascii=False,
            )
            await self._client.publish(
                self.config.publish_topic,
                payload=payload.encode(),
                qos=self.config.qos,
            )
            logger.debug("MQTT → [{}]: {}", msg.chat_id, msg.content[:80])
        except Exception as exc:
            logger.error("MQTT: failed to publish reply: {}", exc)

    async def _process_message(self, message: Any) -> None:
        """Parse an incoming MQTT message and forward it to the bus."""
        try:
            raw = message.payload
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("MQTT: invalid JSON payload: {}", exc)
            return

        sender_id = str(data.get("sender_id", "mqtt_client"))
        chat_id   = str(data.get("chat_id",   "default"))
        content   = str(data.get("content",   "")).strip()
        metadata  = data.get("metadata") or {}

        if not content:
            logger.warning("MQTT: message with empty content, skipping")
            return

        logger.info(
            "MQTT ← [{}] {}: {}",
            chat_id, sender_id, content[:80],
        )

        await self._handle_message(
            sender_id=sender_id,
            chat_id=chat_id,
            content=content,
            metadata={"_mqtt_topic": str(message.topic), **metadata},
        )
