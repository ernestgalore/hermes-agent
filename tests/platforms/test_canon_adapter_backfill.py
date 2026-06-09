"""Regression tests for the Canon adapter inbound-reconnect backfill.

The keystone reliability fix: when the SSE stream drops (Cloud Run recycle /
expired replay window), the adapter must catch up on messages that arrived in
the gap via REST — WITHOUT replaying pre-connect history as fresh turns.
"""

import pytest

from plugins.platforms.canon.adapter import CanonAdapter, _message_created_ms


class _FakeClient:
    def __init__(self, messages_by_convo):
        self._messages = messages_by_convo

    async def get_messages(self, conversation_id, *, limit=50):
        return list(self._messages.get(conversation_id, []))


def _make_adapter():
    adapter = CanonAdapter(config={})
    adapter._agent_id = "agent-x"

    async def _noop_refresh():
        return None

    adapter._refresh_conversations = _noop_refresh  # type: ignore[assignment]
    handled: list[str] = []

    async def _record(payload):
        message_id = payload["message"]["id"]
        handled.append(message_id)
        adapter._remember_seen(message_id)

    adapter._handle_message_payload = _record  # type: ignore[assignment]
    return adapter, handled


def test_message_created_ms_parses_known_formats():
    assert _message_created_ms({"createdAt": 1_700_000_000_000}) == 1_700_000_000_000
    assert _message_created_ms({"createdAt": 1_700_000_000}) == 1_700_000_000_000  # seconds
    assert _message_created_ms({"createdAt": "2026-06-09T12:00:00Z"}) is not None
    assert _message_created_ms({"createdAt": {"_seconds": 1_700_000_000}}) == 1_700_000_000_000
    assert _message_created_ms({"createdAt": None}) is None
    assert _message_created_ms({"createdAt": "not-a-date"}) is None


# Realistic epoch-ms timestamps (> 1e12) so the seconds-vs-ms heuristic in
# _message_created_ms treats them as milliseconds, matching production data.
_BASE_MS = 1_700_000_000_000


@pytest.mark.asyncio
async def test_backfill_replays_only_the_gap_not_history():
    adapter, handled = _make_adapter()
    adapter._client = _FakeClient(
        {
            "c1": [
                {"id": "old", "senderId": "user-1", "text": "history", "createdAt": _BASE_MS},
                {"id": "gap2", "senderId": "user-1", "text": "during gap b", "createdAt": _BASE_MS + 3000},
                {"id": "gap1", "senderId": "user-1", "text": "during gap a", "createdAt": _BASE_MS + 2000},
            ]
        }
    )
    adapter._conversation_cache = {"c1": {"id": "c1"}}
    # Disconnect happened after the agent had seen the message at +1000.
    adapter._last_message_ms = _BASE_MS + 1000

    await adapter._backfill_missed_messages()

    # 'old' (<= high-water mark) is NOT replayed; gap messages are, oldest-first.
    assert handled == ["gap1", "gap2"]


@pytest.mark.asyncio
async def test_backfill_skips_already_seen_and_unparseable_timestamps():
    adapter, handled = _make_adapter()
    adapter._client = _FakeClient(
        {
            "c1": [
                {"id": "seen", "senderId": "u", "text": "x", "createdAt": _BASE_MS + 5000},
                {"id": "noms", "senderId": "u", "text": "x", "createdAt": None},
                {"id": "fresh", "senderId": "u", "text": "x", "createdAt": _BASE_MS + 6000},
            ]
        }
    )
    adapter._conversation_cache = {"c1": {"id": "c1"}}
    adapter._last_message_ms = _BASE_MS
    adapter._remember_seen("seen")  # already processed live before the drop

    await adapter._backfill_missed_messages()

    # 'seen' deduped, 'noms' skipped (unparseable -> never risk replaying), 'fresh' processed.
    assert handled == ["fresh"]


@pytest.mark.asyncio
async def test_backfill_is_a_noop_on_a_fresh_baseline():
    adapter, handled = _make_adapter()
    adapter._client = _FakeClient(
        {
            "c1": [
                {"id": "h1", "senderId": "u", "text": "x", "createdAt": _BASE_MS},
                {"id": "h2", "senderId": "u", "text": "x", "createdAt": _BASE_MS + 2000},
            ]
        }
    )
    adapter._conversation_cache = {"c1": {"id": "c1"}}
    # Baseline set to "now" at connect — well after all existing messages.
    adapter._last_message_ms = _BASE_MS + 1_000_000

    await adapter._backfill_missed_messages()

    assert handled == []
