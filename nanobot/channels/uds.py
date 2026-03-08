"""Unix Domain Socket channel for local process communication.

Allows any local process (e.g. detect_video.py) to push messages
into the nanobot agent loop without HTTP overhead.

Protocol: newline-delimited JSON (NDJSON).

Client → nanobot (inbound):
    {"sender_id": "detector", "chat_id": "alerts", "content": "检测到 person", "metadata": {...}}

Nanobot → client (reply):
    {"chat_id": "alerts", "content": "已收到，正在处理..."}
"""

import asyncio
import json
import os
from typing import Any

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel


class UdsChannel(BaseChannel):
    """Unix Domain Socket channel."""

    name: str = "uds"

    def __init__(self, config: Any, bus: MessageBus):
        super().__init__(config, bus)
        self._server: asyncio.Server | None = None
        # chat_id → StreamWriter, for routing replies back to the right client
        self._writers: dict[str, asyncio.StreamWriter] = {}

    async def start(self) -> None:
        socket_path: str = self.config.socket_path

        # Remove stale socket file if left over from a previous crash
        if os.path.exists(socket_path):
            os.unlink(socket_path)

        self._running = True
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=socket_path,
        )
        logger.info("UDS channel listening on {}", socket_path)

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        socket_path = getattr(self.config, "socket_path", "/tmp/nanobot_uds.sock")
        try:
            os.unlink(socket_path)
        except OSError:
            pass

        logger.info("UDS channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """Route nanobot reply back to the originating client connection."""
        writer = self._writers.get(msg.chat_id)
        if writer is None:
            logger.debug("UDS: no active connection for chat_id={}", msg.chat_id)
            return

        try:
            payload = json.dumps(
                {"chat_id": msg.chat_id, "content": msg.content},
                ensure_ascii=False,
            ) + "\n"
            writer.write(payload.encode())
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            logger.warning("UDS: client disconnected (chat_id={})", msg.chat_id)
            self._writers.pop(msg.chat_id, None)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.info("UDS: new client connected")
        active_chat_id: str | None = None

        try:
            async for raw in reader:
                line = raw.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning("UDS: invalid JSON: {}", exc)
                    continue

                sender_id = str(data.get("sender_id", "local"))
                chat_id   = str(data.get("chat_id",   "default"))
                content   = str(data.get("content",   "")).strip()
                metadata  = data.get("metadata") or {}

                if not content:
                    logger.warning("UDS: message with empty content, skipping")
                    continue

                # Register this writer so we can reply to this chat_id
                self._writers[chat_id] = writer
                active_chat_id = chat_id

                logger.info(
                    "UDS ← [{}] {}: {}",
                    chat_id, sender_id, content[:80]
                )

                await self._handle_message(
                    sender_id=sender_id,
                    chat_id=chat_id,
                    content=content,
                    metadata={"_uds_source": True, **metadata},
                )

        except asyncio.IncompleteReadError:
            pass
        except Exception as exc:
            logger.error("UDS: handler error: {}", exc)
        finally:
            if active_chat_id and self._writers.get(active_chat_id) is writer:
                self._writers.pop(active_chat_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("UDS: client disconnected")
