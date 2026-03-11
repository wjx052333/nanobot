"""Tests for MqttChannel and MqttConfig."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.mqtt import MqttChannel
from nanobot.config.schema import MqttConfig


# ---------------------------------------------------------------------------
# Config schema
# ---------------------------------------------------------------------------


def test_mqtt_config_defaults() -> None:
    cfg = MqttConfig()
    assert cfg.enabled is False
    assert cfg.broker_host == "localhost"
    assert cfg.broker_port == 1883
    assert cfg.client_id == "nanobot"
    assert cfg.subscribe_topic == "nanobot/inbound"
    assert cfg.publish_topic == "nanobot/outbound"
    assert cfg.qos == 1
    assert cfg.allow_from == ["*"]


def test_mqtt_config_camel_case() -> None:
    cfg = MqttConfig.model_validate({
        "enabled": True,
        "brokerHost": "mqtt.example.com",
        "brokerPort": 8883,
        "clientId": "my-bot",
        "subscribeTopic": "bot/in",
        "publishTopic": "bot/out",
        "allowFrom": ["device_01"],
    })
    assert cfg.enabled is True
    assert cfg.broker_host == "mqtt.example.com"
    assert cfg.broker_port == 8883
    assert cfg.client_id == "my-bot"
    assert cfg.subscribe_topic == "bot/in"
    assert cfg.publish_topic == "bot/out"
    assert cfg.allow_from == ["device_01"]


def test_mqtt_config_snake_case() -> None:
    cfg = MqttConfig.model_validate({
        "enabled": True,
        "broker_host": "broker.local",
        "broker_port": 1883,
    })
    assert cfg.broker_host == "broker.local"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_channel(allow_from: list[str] | None = None) -> MqttChannel:
    cfg = MqttConfig(
        enabled=True,
        broker_host="localhost",
        broker_port=1883,
        subscribe_topic="nanobot/inbound",
        publish_topic="nanobot/outbound",
        allow_from=allow_from if allow_from is not None else ["*"],
    )
    bus = MagicMock(spec=MessageBus)
    bus.publish_inbound = AsyncMock()
    channel = MqttChannel(cfg, bus)
    return channel


def _make_mqtt_message(payload: dict) -> MagicMock:
    msg = MagicMock()
    msg.payload = json.dumps(payload).encode()
    msg.topic = MagicMock()
    msg.topic.__str__ = lambda self: "nanobot/inbound"
    return msg


# ---------------------------------------------------------------------------
# send()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_publishes_to_broker() -> None:
    channel = _make_channel()
    mock_client = AsyncMock()
    channel._client = mock_client

    msg = OutboundMessage(channel="mqtt", chat_id="alerts", content="已处理")
    await channel.send(msg)

    mock_client.publish.assert_awaited_once()
    call_kwargs = mock_client.publish.call_args
    assert call_kwargs[0][0] == "nanobot/outbound"
    payload = json.loads(call_kwargs[1]["payload"].decode())
    assert payload["chat_id"] == "alerts"
    assert payload["content"] == "已处理"


@pytest.mark.asyncio
async def test_send_drops_when_not_connected() -> None:
    channel = _make_channel()
    channel._client = None  # not connected

    msg = OutboundMessage(channel="mqtt", chat_id="alerts", content="hello")
    # Should not raise
    await channel.send(msg)


# ---------------------------------------------------------------------------
# _process_message()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_message_dispatches_to_bus() -> None:
    channel = _make_channel(allow_from=["*"])
    raw_msg = _make_mqtt_message({
        "sender_id": "sensor_01",
        "chat_id": "room_alerts",
        "content": "temperature=35",
    })

    await channel._process_message(raw_msg)

    channel.bus.publish_inbound.assert_awaited_once()
    inbound = channel.bus.publish_inbound.call_args[0][0]
    assert inbound.sender_id == "sensor_01"
    assert inbound.chat_id == "room_alerts"
    assert inbound.content == "temperature=35"
    assert inbound.channel == "mqtt"


@pytest.mark.asyncio
async def test_process_message_skips_empty_content() -> None:
    channel = _make_channel()
    raw_msg = _make_mqtt_message({"sender_id": "s", "chat_id": "c", "content": ""})

    await channel._process_message(raw_msg)

    channel.bus.publish_inbound.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_invalid_json() -> None:
    channel = _make_channel()
    msg = MagicMock()
    msg.payload = b"not-json"
    msg.topic = MagicMock()

    await channel._process_message(msg)

    channel.bus.publish_inbound.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_denied_sender() -> None:
    channel = _make_channel(allow_from=["allowed_device"])
    raw_msg = _make_mqtt_message({
        "sender_id": "unknown_device",
        "chat_id": "alerts",
        "content": "hello",
    })

    await channel._process_message(raw_msg)

    channel.bus.publish_inbound.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_includes_topic_in_metadata() -> None:
    channel = _make_channel(allow_from=["*"])
    raw_msg = _make_mqtt_message({
        "sender_id": "dev",
        "chat_id": "c",
        "content": "ping",
    })

    await channel._process_message(raw_msg)

    inbound = channel.bus.publish_inbound.call_args[0][0]
    assert "_mqtt_topic" in inbound.metadata
