"""
Canon platform adapter for Hermes Agent.

This bundled plugin connects Hermes to Canon agent conversations using the
same public surface as Canon's TypeScript SDK:

* REST endpoints for identity, conversation discovery, sends, and typing.
* Server-sent events from /agents/stream?events=messages for inbound messages.

The adapter is intentionally dependency-light: Hermes already depends on
httpx, so we do not need a Node sidecar or a runtime dependency on
@canonmsg/* packages.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import mimetypes
import os
import re
import stat
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Deque, Dict, Optional

import httpx

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
    cache_video_from_bytes,
    safe_url_for_log,
    _ssrf_redirect_guard,
)

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://api-6m6mlelskq-uc.a.run.app"
DEFAULT_STREAM_URL = "https://canon-agent-stream-195218560334.us-central1.run.app"
DEFAULT_HISTORY_LIMIT = 50
DEFAULT_TIMEOUT_SECONDS = 30.0
MAX_SEEN_MESSAGE_IDS = 1024
MAX_MEDIA_BYTES = 10 * 1024 * 1024
STREAM_BACKOFF_MAX_SECONDS = 60.0
STREAM_ERROR_LOG_DEDUPE_SECONDS = 60.0
DEFAULT_APPROVAL_TIMEOUT_SECONDS = 300
DEFAULT_RUNTIME_INPUT_TIMEOUT_SECONDS = 600

AUDIO_EXTS = {".m4a", ".mp3", ".ogg", ".opus", ".wav", ".webm", ".flac"}
IMAGE_EXTS = {".gif", ".jpeg", ".jpg", ".png", ".webp"}
VIDEO_EXTS = {".avi", ".mkv", ".mov", ".mp4", ".webm", ".3gp"}
SECRET_PATTERN = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s'\"`]+"
)

TURN_COMPLETE_METADATA = {
    "turnSemantics": "turn_complete",
    "turnComplete": True,
}


class CanonApiError(Exception):
    """HTTP error from Canon's agent API."""

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body.strip()
        self.payload = _json_object_from_text(self.body)
        self.code = _first_string(self.payload, "code") if self.payload else None
        self.retry_after_ms = _int_from_dict(self.payload, "retryAfterMs")
        super().__init__(f"Canon API returned {status_code}: {self.body[:500]}")

    @property
    def retryable(self) -> bool:
        return self.status_code in {408, 425, 429} or self.status_code >= 500


class CanonStreamError(Exception):
    """Structured SSE error frame from Canon's agent stream."""

    def __init__(
        self,
        payload: Any,
        *,
        status_code: Optional[int] = None,
    ) -> None:
        body = payload if isinstance(payload, dict) else {"message": str(payload)}
        self.payload = body
        self.status_code = status_code
        self.code = _first_string(body, "code") or "stream_error"
        self.message = _first_string(body, "message") or self.code
        self.retry_after_ms = _int_from_dict(body, "retryAfterMs")
        self.active_connections = _int_from_dict(body, "activeConnections")
        self.max_connections = _int_from_dict(body, "maxConnections")
        super().__init__(self.message)

    @property
    def retryable(self) -> bool:
        return True

    @property
    def retry_after_seconds(self) -> Optional[float]:
        if self.retry_after_ms is None:
            return None
        return max(1.0, self.retry_after_ms / 1000.0)


@dataclass
class CanonStreamFrame:
    event: str
    data: Any
    event_id: Optional[str] = None


@dataclass
class PendingApproval:
    conversation_id: str
    session_key: str
    command: str
    description: str
    created_at: float


@dataclass
class PendingRuntimeInput:
    conversation_id: str
    session_key: str
    input_id: str
    kind: str
    created_at: float
    clarify_id: Optional[str] = None


class CanonHttpClient:
    """Small async client for the Canon agent REST and SSE APIs."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        stream_url: str = DEFAULT_STREAM_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.stream_url = stream_url.rstrip("/")
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            event_hooks={"response": [_ssrf_redirect_guard]},
        )

    def _headers(self, *, accept: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if accept:
            headers["Accept"] = accept
        return headers

    async def close(self) -> None:
        await self._client.aclose()

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        res = await self._client.request(
            method,
            f"{self.base_url}{path}",
            headers=self._headers(),
            params=params,
            json=json_body,
        )
        if res.status_code >= 400:
            raise CanonApiError(res.status_code, res.text)
        if not res.content:
            return {}
        return res.json()

    async def get_me(self) -> dict[str, Any]:
        data = await self._request_json("GET", "/agents/me")
        return data if isinstance(data, dict) else {}

    async def get_conversations(self) -> list[dict[str, Any]]:
        data = await self._request_json("GET", "/conversations")
        conversations = data.get("conversations") if isinstance(data, dict) else data
        return conversations if isinstance(conversations, list) else []

    async def get_messages(
        self, conversation_id: str, *, limit: int = DEFAULT_HISTORY_LIMIT
    ) -> list[dict[str, Any]]:
        data = await self._request_json(
            "GET",
            f"/conversations/{conversation_id}/messages",
            params={"limit": str(limit)},
        )
        messages = data.get("messages") if isinstance(data, dict) else data
        return messages if isinstance(messages, list) else []

    async def send_message(
        self,
        conversation_id: str,
        text: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "conversationId": conversation_id,
            "text": text,
        }
        if options:
            body.update(options)
        if reply_to:
            body["replyTo"] = reply_to
        if metadata:
            body["metadata"] = metadata

        data = await self._request_json("POST", "/messages/send", json_body=body)
        return data if isinstance(data, dict) else {}

    async def upload_media(
        self,
        conversation_id: str,
        data: str,
        mime_type: str,
        *,
        file_name: Optional[str] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "conversationId": conversation_id,
            "data": data,
            "mimeType": mime_type,
        }
        if file_name:
            body["fileName"] = file_name
        result = await self._request_json("POST", "/media/upload", json_body=body)
        return result if isinstance(result, dict) else {}

    async def download_media(self, url: str) -> tuple[bytes, Optional[str]]:
        from tools.url_safety import is_safe_url

        if not is_safe_url(url):
            raise ValueError(
                f"Blocked unsafe URL (SSRF protection): {safe_url_for_log(url)}"
            )

        res = await self._client.get(
            url,
            headers={"User-Agent": "HermesAgent/CanonPlatform"},
            follow_redirects=True,
        )
        if res.status_code >= 400:
            raise CanonApiError(res.status_code, res.text)
        if len(res.content) > MAX_MEDIA_BYTES:
            raise ValueError("Canon media attachment exceeds 10MB")
        return res.content, res.headers.get("content-type")

    async def set_typing(
        self,
        conversation_id: str,
        typing: bool,
        status: Optional[str] = None,
    ) -> None:
        body: dict[str, Any] = {
            "conversationId": conversation_id,
            "typing": typing,
        }
        if status in {"typing", "thinking"}:
            body["status"] = status
        await self._request_json("POST", "/typing", json_body=body)

    async def create_runtime_input_request(
        self,
        conversation_id: str,
        *,
        input_id: str,
        kind: str,
        expires_at: int,
    ) -> None:
        await self._request_json(
            "POST",
            "/runtime-input/request",
            json_body={
                "conversationId": conversation_id,
                "inputId": input_id,
                "kind": kind,
                "expiresAt": expires_at,
            },
        )

    async def consume_runtime_input_response(
        self,
        conversation_id: str,
        *,
        input_id: str,
        cancel: bool = False,
    ) -> dict[str, Any]:
        data = await self._request_json(
            "POST",
            "/runtime-input/consume",
            json_body={
                "conversationId": conversation_id,
                "inputId": input_id,
                **({"cancel": True} if cancel else {}),
            },
        )
        return data if isinstance(data, dict) else {}

    async def stream_events(
        self,
        *,
        last_event_id: Optional[str] = None,
    ) -> AsyncIterator[CanonStreamFrame]:
        url = f"{self.stream_url}/agents/stream"
        headers = self._headers(accept="text/event-stream")
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        async with self._client.stream(
            "GET",
            url,
            headers=headers,
            params={"events": "messages"},
            timeout=None,
        ) as res:
            if res.status_code >= 400:
                body = await res.aread()
                text = body.decode("utf-8", errors="replace")
                payload = _json_object_from_text(text)
                if payload and _first_string(payload, "code"):
                    raise CanonStreamError(payload, status_code=res.status_code)
                raise CanonApiError(res.status_code, text)

            buffer = ""
            async for chunk in res.aiter_text():
                if not chunk:
                    continue
                buffer += chunk.replace("\r\n", "\n").replace("\r", "\n")
                while "\n\n" in buffer:
                    frame, buffer = buffer.split("\n\n", 1)
                    parsed = _parse_sse_frame(frame)
                    if parsed is not None:
                        if parsed.event == "error":
                            raise CanonStreamError(parsed.data)
                        yield parsed


class CanonAdapter(BasePlatformAdapter):
    """Hermes gateway adapter for Canon conversations."""

    def __init__(self, config, **_: Any) -> None:
        super().__init__(config=config, platform=Platform("canon"))

        self.api_key = _config_value(config, "api_key", "CANON_API_KEY")
        self.base_url = _config_value(
            config, "base_url", "CANON_BASE_URL", DEFAULT_BASE_URL
        )
        self.stream_url = _config_value(
            config, "stream_url", "CANON_STREAM_URL", DEFAULT_STREAM_URL
        )
        self.history_limit = _config_int(
            config, "history_limit", "CANON_HISTORY_LIMIT", DEFAULT_HISTORY_LIMIT
        )

        self._client: Optional[CanonHttpClient] = None
        self._agent_id: Optional[str] = None
        self._owner_id: Optional[str] = None
        self._stream_task: Optional[asyncio.Task] = None
        self._stream_stop: Optional[asyncio.Event] = None
        self._last_event_id: Optional[str] = None
        self._conversation_cache: dict[str, dict[str, Any]] = {}
        self._seen_message_ids: set[str] = set()
        self._seen_message_order: Deque[str] = deque()
        self._pending_approvals: dict[str, PendingApproval] = {}
        self._pending_runtime_inputs: dict[str, PendingRuntimeInput] = {}
        self._last_stream_warning: tuple[str, float] | None = None

    @property
    def name(self) -> str:
        return "Canon"

    async def connect(self) -> bool:
        if not self.api_key:
            self._set_fatal_error(
                "missing_api_key",
                _missing_api_key_message(config),
                retryable=False,
            )
            return False

        self._client = self._make_client()
        self._stream_stop = asyncio.Event()

        try:
            ctx = await self._client.get_me()
            self._agent_id = _first_string(ctx, "agentId", "id", "userId")
            self._owner_id = _first_string(ctx, "ownerId")
            await self._refresh_conversations()
            self._mark_connected()
            self._stream_task = asyncio.create_task(
                self._stream_loop(), name="canon-platform-stream"
            )
            logger.info(
                "Canon platform connected as agent %s", self._agent_id or "<unknown>"
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._set_fatal_error(
                "connect_failed", _safe_error(exc), retryable=_is_retryable(exc)
            )
            await self._close_client()
            return False

    async def disconnect(self) -> None:
        stop = self._stream_stop
        if stop is not None:
            stop.set()

        task = self._stream_task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug(
                    "Canon stream task raised during disconnect", exc_info=True
                )

        self._stream_task = None
        self._stream_stop = None
        await self._close_client()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self.api_key:
            return SendResult(
                success=False, error="Canon API key is not configured", retryable=False
            )

        client = self._client or self._make_client()
        owns_client = self._client is None

        try:
            canon_options, message_metadata = _split_canon_metadata(metadata)
            message_metadata.update(TURN_COMPLETE_METADATA)
            data = await client.send_message(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=message_metadata,
                options=canon_options,
            )
            message_id = _first_string(data, "messageId", "id")
            return SendResult(
                success=True,
                message_id=message_id,
                raw_response=data,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False,
                error=_safe_error(exc),
                retryable=_is_retryable(exc),
            )
        finally:
            if owns_client:
                await client.close()

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_attachment(
            chat_id,
            caption or "",
            content_type="image",
            attachment={"kind": "image", "url": image_url},
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media_file(
            chat_id,
            image_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
            content_type="image",
            mime_hint=kwargs.get("mime_type"),
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media_file(
            chat_id,
            audio_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
            content_type="audio",
            mime_hint=kwargs.get("mime_type"),
            duration_ms=kwargs.get("duration_ms"),
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media_file(
            chat_id,
            video_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
            content_type="file",
            mime_hint=kwargs.get("mime_type"),
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media_file(
            chat_id,
            file_path,
            caption=caption,
            file_name=file_name,
            reply_to=reply_to,
            metadata=metadata,
            content_type="file",
            mime_hint=kwargs.get("mime_type"),
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if not self.api_key:
            return
        client = self._client or self._make_client()
        owns_client = self._client is None
        try:
            status = "thinking"
            if isinstance(metadata, dict) and metadata.get("status") in {
                "typing",
                "thinking",
            }:
                status = metadata["status"]
            await client.set_typing(chat_id, True, status)
        except Exception:
            logger.debug("Canon typing indicator failed", exc_info=True)
        finally:
            if owns_client:
                await client.close()

    async def stop_typing(self, chat_id: str) -> None:
        if not self.api_key:
            return
        client = self._client or self._make_client()
        owns_client = self._client is None
        try:
            await client.set_typing(chat_id, False)
        except Exception:
            logger.debug("Canon typing clear failed", exc_info=True)
        finally:
            if owns_client:
                await client.close()

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render a Canon approval card for Hermes dangerous-command approval."""
        approval_id = f"apr_{uuid.uuid4().hex[:16]}"
        summary = _redact_text(command).strip()[:1000] or "Command execution"
        risk = _approval_risk_for_command(command)
        expires_at = _iso_after(DEFAULT_APPROVAL_TIMEOUT_SECONDS)
        request_metadata = {
            "type": "approval_request",
            "approvalId": approval_id,
            "toolName": "Shell",
            "toolSummary": summary,
            "category": "command",
            "risk": risk,
            "riskLevel": "destructive" if risk == "destructive" else "normal",
            "runtimeId": "hermes",
            "turnId": session_key,
            "native": {
                "runtime": "hermes",
                "method": "send_exec_approval",
                "requestId": approval_id,
                "sessionKey": session_key,
                "handles": {
                    "approvalId": approval_id,
                    "sessionKey": session_key,
                },
            },
            "details": [
                {"label": "Command", "value": summary, "monospace": True},
                {"label": "Reason", "value": _redact_text(description)[:500]},
            ],
            "expiresAt": expires_at,
        }
        text = _approval_request_text(
            approval_id=approval_id,
            command=summary,
            description=_redact_text(description),
            expires_at=expires_at,
        )
        result = await self._send_control_message(
            chat_id,
            text,
            metadata=request_metadata,
            parent_metadata=metadata,
        )
        if result.success:
            self._pending_approvals[approval_id] = PendingApproval(
                conversation_id=str(chat_id),
                session_key=session_key,
                command=summary,
                description=_redact_text(description),
                created_at=time.monotonic(),
            )
        return result

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render a Canon runtime input card for Hermes clarify prompts."""
        input_id = f"rin_{clarify_id}"
        normalized_choices = _runtime_input_choices(choices)
        expires_at_ms = _ms_after(DEFAULT_RUNTIME_INPUT_TIMEOUT_SECONDS)
        expires_at = _iso_from_ms(expires_at_ms)
        client = self._client or self._make_client()
        owns_client = self._client is None
        try:
            await client.create_runtime_input_request(
                str(chat_id),
                input_id=input_id,
                kind="clarify",
                expires_at=expires_at_ms,
            )
        except Exception as exc:
            if owns_client:
                await client.close()
            return SendResult(
                success=False,
                error=_safe_error(exc),
                retryable=_is_retryable(exc),
            )

        request_metadata = {
            "type": "runtime_input_request",
            "inputId": input_id,
            "kind": "clarify",
            "prompt": _redact_text(question)[:1000],
            "title": "Input required",
            "native": {
                "runtime": "hermes",
                "method": "send_clarify",
                "requestId": clarify_id,
                "sessionKey": session_key,
                "handles": {
                    "inputId": input_id,
                    "clarifyId": clarify_id,
                    "sessionKey": session_key,
                },
            },
            "expiresAt": expires_at,
        }
        if normalized_choices:
            request_metadata["choices"] = normalized_choices

        # Keep Hermes' existing text fallback alive: a plain Canon text reply in
        # the same conversation can still satisfy the pending clarify prompt.
        try:
            from tools.clarify_gateway import mark_awaiting_text

            mark_awaiting_text(clarify_id)
        except Exception:
            logger.debug("Could not mark Canon clarify as awaiting text", exc_info=True)

        result = await self._send_control_message(
            chat_id,
            "Input required",
            metadata=request_metadata,
            parent_metadata=metadata,
            client=client,
        )
        if not result.success:
            try:
                await client.consume_runtime_input_response(
                    str(chat_id),
                    input_id=input_id,
                    cancel=True,
                )
            except Exception:
                pass
        if owns_client:
            await client.close()
        if result.success:
            self._pending_runtime_inputs[input_id] = PendingRuntimeInput(
                conversation_id=str(chat_id),
                session_key=session_key,
                input_id=input_id,
                clarify_id=clarify_id,
                kind="clarify",
                created_at=time.monotonic(),
            )
        return result

    async def request_runtime_input(
        self,
        chat_id: str,
        *,
        kind: str,
        prompt: str,
        input_id: str,
        session_key: str,
        title: Optional[str] = None,
        choices: Optional[list] = None,
        secret_name: Optional[str] = None,
        timeout_seconds: float = DEFAULT_RUNTIME_INPUT_TIMEOUT_SECONDS,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Render a Canon runtime input card and wait for the owner response."""
        if kind not in {"clarify", "sudo", "secret"}:
            return None
        client = self._client or self._make_client()
        owns_client = self._client is None
        status = "timeout"
        pending = PendingRuntimeInput(
            conversation_id=str(chat_id),
            session_key=session_key,
            input_id=input_id,
            kind=kind,
            created_at=time.monotonic(),
        )

        try:
            expires_at_ms = _ms_after(timeout_seconds)
            expires_at = _iso_from_ms(expires_at_ms)
            await client.create_runtime_input_request(
                str(chat_id),
                input_id=input_id,
                kind=kind,
                expires_at=expires_at_ms,
            )

            request_metadata: dict[str, Any] = {
                "type": "runtime_input_request",
                "inputId": input_id,
                "kind": kind,
                "prompt": _redact_text(prompt)[:1000] if kind == "clarify" else prompt[:1000],
                "title": title or _runtime_input_title(kind, secret_name),
                "native": {
                    "runtime": "hermes",
                    "method": f"{kind}.request",
                    "requestId": input_id,
                    "sessionKey": session_key,
                    "handles": {
                        "inputId": input_id,
                        "sessionKey": session_key,
                    },
                },
                "expiresAt": expires_at,
            }
            normalized_choices = _runtime_input_choices(choices)
            if normalized_choices:
                request_metadata["choices"] = normalized_choices
            if secret_name:
                request_metadata["secretName"] = secret_name
            if kind in {"sudo", "secret"}:
                request_metadata["sensitive"] = True

            result = await self._send_control_message(
                chat_id,
                "Input required",
                metadata=request_metadata,
                parent_metadata=metadata,
                client=client,
            )
            if not result.success:
                await client.consume_runtime_input_response(
                    str(chat_id), input_id=input_id, cancel=True
                )
                status = "cancelled"
                return None

            self._pending_runtime_inputs[input_id] = pending
            deadline = time.monotonic() + max(1.0, float(timeout_seconds))
            while time.monotonic() < deadline:
                response = await client.consume_runtime_input_response(
                    str(chat_id), input_id=input_id
                )
                response_status = response.get("status")
                if response_status == "submitted":
                    status = "submitted"
                    value = response.get("value")
                    return value if isinstance(value, str) else ""
                if response_status in {"cancelled", "timeout"}:
                    status = str(response_status)
                    return None
                await asyncio.sleep(1.0)

            try:
                await client.consume_runtime_input_response(str(chat_id), input_id=input_id)
            except Exception:
                pass
            return None
        except asyncio.CancelledError:
            try:
                await client.consume_runtime_input_response(
                    str(chat_id), input_id=input_id, cancel=True
                )
            except Exception:
                pass
            status = "cancelled"
            raise
        except Exception:
            logger.debug("Canon runtime input request failed", exc_info=True)
            return None
        finally:
            self._pending_runtime_inputs.pop(input_id, None)
            try:
                await self._send_runtime_input_outcome(input_id, pending, status)
            except Exception:
                pass
            if owns_client:
                await client.close()

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        convo = self._conversation_cache.get(str(chat_id))
        if convo is None and self._client is not None:
            await self._refresh_conversations()
            convo = self._conversation_cache.get(str(chat_id))
        return {
            "id": str(chat_id),
            "name": _conversation_name(convo) if convo else str(chat_id),
            "type": _chat_type_from_conversation(convo),
        }

    async def _send_control_message(
        self,
        chat_id: str,
        text: str,
        *,
        metadata: dict[str, Any],
        parent_metadata: Optional[Dict[str, Any]] = None,
        client: Optional[CanonHttpClient] = None,
    ) -> SendResult:
        if not self.api_key:
            return SendResult(
                success=False, error="Canon API key is not configured", retryable=False
            )

        active_client = client or self._client or self._make_client()
        owns_client = client is None and self._client is None
        try:
            canon_options, extra_metadata = _split_canon_metadata(parent_metadata)
            message_metadata = {**extra_metadata, **metadata}
            canon_options.setdefault("contentType", "text")
            data = await active_client.send_message(
                str(chat_id),
                text,
                metadata=message_metadata,
                options=canon_options,
            )
            return SendResult(
                success=True,
                message_id=_first_string(data, "messageId", "id"),
                raw_response=data,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False,
                error=_safe_error(exc),
                retryable=_is_retryable(exc),
            )
        finally:
            if owns_client:
                await active_client.close()

    def _make_client(self) -> CanonHttpClient:
        return CanonHttpClient(
            self.api_key,
            base_url=self.base_url,
            stream_url=self.stream_url,
        )

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.close()

    async def _refresh_conversations(self) -> None:
        if self._client is None:
            return
        conversations = await self._client.get_conversations()
        for convo in conversations:
            convo_id = _first_string(convo, "id", "conversationId")
            if convo_id:
                self._conversation_cache[convo_id] = convo

    async def _send_media_file(
        self,
        chat_id: str,
        file_path: str,
        *,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: str,
        mime_hint: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> SendResult:
        if not self.api_key:
            return SendResult(
                success=False, error="Canon API key is not configured", retryable=False
            )

        path = Path(file_path).expanduser()
        if not path.exists():
            return SendResult(success=False, error=f"Media file not found: {file_path}")
        if path.stat().st_size > MAX_MEDIA_BYTES:
            return SendResult(success=False, error="Canon media upload limit is 10MB")

        client = self._client or self._make_client()
        owns_client = self._client is None
        try:
            media_bytes = path.read_bytes()
            mime_type = _guess_mime_type(str(path), mime_hint)
            uploaded = await client.upload_media(
                chat_id,
                base64.b64encode(media_bytes).decode("ascii"),
                mime_type,
                file_name=file_name or path.name,
            )
            attachment = dict(uploaded.get("attachment") or {})
            if not attachment:
                attachment = {
                    "kind": _canon_attachment_kind_for_mime(mime_type),
                    "url": uploaded.get("url"),
                    "mimeType": mime_type,
                    "fileName": file_name or path.name,
                    "sizeBytes": len(media_bytes),
                }
            if duration_ms and attachment.get("kind") == "audio":
                attachment["durationMs"] = int(duration_ms)
            if mime_type.startswith("video/"):
                content_type = "file"
            elif attachment.get("kind") in {"image", "audio"}:
                content_type = str(attachment["kind"])

            return await self._send_attachment(
                chat_id,
                caption or "",
                content_type=content_type,
                attachment=attachment,
                reply_to=reply_to,
                metadata=metadata,
                client=client,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False, error=_safe_error(exc), retryable=_is_retryable(exc)
            )
        finally:
            if owns_client:
                await client.close()

    async def _send_attachment(
        self,
        chat_id: str,
        text: str,
        *,
        content_type: str,
        attachment: dict[str, Any],
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        client: Optional[CanonHttpClient] = None,
    ) -> SendResult:
        if not self.api_key:
            return SendResult(
                success=False, error="Canon API key is not configured", retryable=False
            )

        active_client = client or self._client or self._make_client()
        owns_client = client is None and self._client is None
        try:
            canon_options, message_metadata = _split_canon_metadata(metadata)
            message_metadata.update(TURN_COMPLETE_METADATA)
            canon_options.update({
                "contentType": content_type,
                "attachments": [attachment],
            })
            data = await active_client.send_message(
                chat_id,
                text,
                reply_to=reply_to,
                metadata=message_metadata,
                options=canon_options,
            )
            return SendResult(
                success=True,
                message_id=_first_string(data, "messageId", "id"),
                raw_response=data,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False, error=_safe_error(exc), retryable=_is_retryable(exc)
            )
        finally:
            if owns_client:
                await active_client.close()

    async def _stream_loop(self) -> None:
        assert self._client is not None
        backoff = 1.0

        while self._stream_stop is not None and not self._stream_stop.is_set():
            try:
                async for frame in self._client.stream_events(
                    last_event_id=self._last_event_id
                ):
                    if frame.event_id:
                        self._last_event_id = frame.event_id
                    await self._handle_stream_frame(frame)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except CanonStreamError as exc:
                if self._stream_stop is not None and self._stream_stop.is_set():
                    break
                delay = exc.retry_after_seconds
                if delay is None:
                    delay = backoff
                    backoff = min(backoff * 2, STREAM_BACKOFF_MAX_SECONDS)
                else:
                    backoff = min(max(backoff * 2, delay), STREAM_BACKOFF_MAX_SECONDS)
                self._log_stream_warning(exc, delay)
                await asyncio.sleep(delay)
            except Exception as exc:
                if self._stream_stop is not None and self._stream_stop.is_set():
                    break
                logger.warning("Canon stream disconnected: %s", _safe_error(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, STREAM_BACKOFF_MAX_SECONDS)

    def _log_stream_warning(self, exc: CanonStreamError, retry_after: float) -> None:
        signature = f"{exc.code}:{exc.retry_after_ms}:{exc.active_connections}:{exc.max_connections}"
        now = time.monotonic()
        last = self._last_stream_warning
        if last and last[0] == signature and now - last[1] < STREAM_ERROR_LOG_DEDUPE_SECONDS:
            return
        self._last_stream_warning = (signature, now)
        details = []
        if exc.active_connections is not None and exc.max_connections is not None:
            details.append(f"active={exc.active_connections}/{exc.max_connections}")
        logger.warning(
            "Canon stream rejected (%s): %s; retrying in %.1fs%s",
            exc.code,
            exc.message,
            retry_after,
            f" ({', '.join(details)})" if details else "",
        )

    async def _handle_stream_frame(self, frame: CanonStreamFrame) -> None:
        if frame.event == "agent.context" and isinstance(frame.data, dict):
            self._agent_id = (
                _first_string(frame.data, "agentId", "id", "userId") or self._agent_id
            )
            return
        if frame.event == "conversation.updated" and isinstance(frame.data, dict):
            convo_id = _first_string(frame.data, "conversationId", "id")
            if convo_id:
                cached = self._conversation_cache.setdefault(convo_id, {"id": convo_id})
                changes = frame.data.get("changes")
                if isinstance(changes, dict):
                    cached.update(changes)
            return
        if frame.event != "message.created":
            return
        if isinstance(frame.data, dict):
            await self._handle_message_payload(frame.data)

    async def _handle_message_payload(self, payload: dict[str, Any]) -> None:
        message = payload.get("message")
        if not isinstance(message, dict):
            return

        sender_id = _first_string(message, "senderId")
        if self._agent_id and sender_id == self._agent_id:
            return

        message_id = _first_string(message, "id")
        if message_id and self._already_seen(message_id):
            return

        conversation_id = _first_string(payload, "conversationId")
        if not conversation_id:
            return

        conversation = payload.get("conversation")
        if isinstance(conversation, dict):
            self._conversation_cache[conversation_id] = conversation
        conversation = self._conversation_cache.get(
            conversation_id, conversation if isinstance(conversation, dict) else None
        )

        if await self._handle_control_reply(conversation_id, message):
            return

        text = _message_text(message)
        if not text:
            return

        (
            media_urls,
            media_types,
            media_message_type,
        ) = await self._materialize_attachments(message)

        source = self.build_source(
            chat_id=conversation_id,
            chat_name=_conversation_name(conversation),
            chat_type=_chat_type_from_conversation(conversation),
            user_id=sender_id,
            user_name=_first_string(message, "senderName"),
            message_id=message_id,
        )

        event = MessageEvent(
            text=text,
            message_type=_message_type_for_text_and_media(text, media_message_type),
            source=source,
            raw_message=payload,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=_first_string(message, "replyTo"),
        )
        await self.handle_message(event)

    async def _handle_control_reply(
        self,
        conversation_id: str,
        message: dict[str, Any],
    ) -> bool:
        metadata = message.get("metadata")
        if not isinstance(metadata, dict):
            return False
        message_type = metadata.get("type")
        if message_type == "approval_reply":
            await self._handle_approval_reply(conversation_id, message, metadata)
            return True
        if message_type == "runtime_input_reply":
            await self._handle_runtime_input_reply(conversation_id, message, metadata)
            return True
        if message_type in {
            "approval_outcome",
            "runtime_input_outcome",
            "question_reply",
            "plan_approval_reply",
        }:
            return True
        return False

    async def _handle_approval_reply(
        self,
        conversation_id: str,
        message: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        approval_id = _first_string(metadata, "approvalId")
        decision = _first_string(metadata, "decision")
        if not approval_id or decision not in {"allow", "deny"}:
            return

        pending = self._pending_approvals.get(approval_id)
        if pending is None or pending.conversation_id != conversation_id:
            return
        if not self._is_owner_reply(message):
            logger.warning("Ignoring non-owner Canon approval reply for %s", approval_id)
            return

        choice = _hermes_approval_choice(decision, metadata.get("sessionRule"))
        try:
            from tools.approval import resolve_gateway_approval

            resolved = resolve_gateway_approval(pending.session_key, choice)
        except Exception:
            logger.exception("Failed to resolve Canon approval reply")
            resolved = 0

        if resolved:
            self._pending_approvals.pop(approval_id, None)
            await self._send_approval_outcome(approval_id, pending, decision, "replied")

    async def _handle_runtime_input_reply(
        self,
        conversation_id: str,
        message: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        input_id = _first_string(metadata, "inputId")
        status = _first_string(metadata, "status")
        if not input_id or status not in {"submitted", "cancelled"}:
            return

        pending = self._pending_runtime_inputs.get(input_id)
        if pending is None or pending.conversation_id != conversation_id:
            return
        if not self._is_owner_reply(message):
            logger.warning("Ignoring non-owner Canon runtime input reply for %s", input_id)
            return

        if pending.kind != "clarify":
            return
        if not pending.clarify_id:
            return

        answer = ""
        if status == "submitted":
            answer = (
                _first_string(metadata, "answerSummary")
                or _message_text(message)
                or ""
            )
        try:
            from tools.clarify_gateway import resolve_gateway_clarify

            resolved = resolve_gateway_clarify(pending.clarify_id, answer)
        except Exception:
            logger.exception("Failed to resolve Canon runtime input reply")
            resolved = False

        if resolved:
            if self._client is not None:
                try:
                    await self._client.consume_runtime_input_response(
                        conversation_id,
                        input_id=input_id,
                    )
                except Exception:
                    logger.debug("Failed to consume Canon clarify value", exc_info=True)
            self._pending_runtime_inputs.pop(input_id, None)
            await self._send_runtime_input_outcome(input_id, pending, status)

    def _is_owner_reply(self, message: dict[str, Any]) -> bool:
        if message.get("isOwner") is True:
            return True
        sender_id = _first_string(message, "senderId")
        return bool(self._owner_id and sender_id and sender_id == self._owner_id)

    async def _send_approval_outcome(
        self,
        approval_id: str,
        pending: PendingApproval,
        decision: str,
        reason: str,
    ) -> None:
        try:
            await self._send_control_message(
                pending.conversation_id,
                _approval_outcome_text(approval_id, pending.command, decision, reason),
                metadata={
                    "type": "approval_outcome",
                    "approvalId": approval_id,
                    "decision": "allow" if decision == "allow" else "deny",
                    "reason": reason,
                },
            )
        except Exception:
            logger.debug("Failed to send Canon approval outcome", exc_info=True)

    async def _send_runtime_input_outcome(
        self,
        input_id: str,
        pending: PendingRuntimeInput,
        status: str,
    ) -> None:
        try:
            await self._send_control_message(
                pending.conversation_id,
                "Input received" if status == "submitted" else "Input cancelled",
                metadata={
                    "type": "runtime_input_outcome",
                    "inputId": input_id,
                    "kind": pending.kind,
                    "status": status,
                    "reason": status,
                },
            )
        except Exception:
            logger.debug("Failed to send Canon runtime input outcome", exc_info=True)

    async def _materialize_attachments(
        self,
        message: dict[str, Any],
    ) -> tuple[list[str], list[str], Optional[MessageType]]:
        attachments = message.get("attachments")
        if not isinstance(attachments, list) or not attachments:
            return [], [], None
        if self._client is None:
            return [], [], None

        media_urls: list[str] = []
        media_types: list[str] = []
        message_type: Optional[MessageType] = None

        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            url = _first_string(attachment, "url")
            if not url:
                continue

            try:
                data, response_mime = await self._client.download_media(url)
                mime_type = _attachment_mime_type(attachment, response_mime)
                local_path, local_message_type = _cache_canon_media(
                    attachment, data, mime_type
                )
            except Exception:
                logger.debug(
                    "Failed to materialize Canon media attachment", exc_info=True
                )
                continue

            media_urls.append(local_path)
            media_types.append(mime_type)
            message_type = _prefer_media_message_type(message_type, local_message_type)

        return media_urls, media_types, message_type

    def _already_seen(self, message_id: str) -> bool:
        if message_id in self._seen_message_ids:
            return True
        self._seen_message_ids.add(message_id)
        self._seen_message_order.append(message_id)
        while len(self._seen_message_order) > MAX_SEEN_MESSAGE_IDS:
            old = self._seen_message_order.popleft()
            self._seen_message_ids.discard(old)
        return False


def _parse_sse_frame(frame: str) -> Optional[CanonStreamFrame]:
    event: Optional[str] = None
    event_id: Optional[str] = None
    data_lines: list[str] = []

    for line in frame.split("\n"):
        if not line or line.startswith(":"):
            continue
        if line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())

    if not event or not data_lines:
        return None

    raw_data = "\n".join(data_lines)
    try:
        data: Any = json.loads(raw_data)
    except json.JSONDecodeError:
        data = raw_data
    return CanonStreamFrame(event=event, data=data, event_id=event_id)


def _json_object_from_text(text: str) -> Optional[dict[str, Any]]:
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _int_from_dict(data: Optional[dict[str, Any]], key: str) -> Optional[int]:
    if not isinstance(data, dict) or key not in data:
        return None
    try:
        return int(data[key])
    except (TypeError, ValueError):
        return None


def _config_value(config: Any, key: str, env_name: str, default: str = "") -> str:
    env_value = os.getenv(env_name)
    if env_value:
        return env_value.strip()

    if key == "api_key":
        for attr in ("api_key", "token"):
            value = getattr(config, attr, None)
            if value:
                return str(value).strip()

    extra = getattr(config, "extra", {}) or {}
    if isinstance(extra, dict):
        for candidate in (key, env_name.lower(), env_name):
            value = extra.get(candidate)
            if value:
                return str(value).strip()

    if key == "api_key":
        profile_name = _configured_profile_name(config)
        if profile_name:
            profile_key = _load_profile_api_key(profile_name)
            if profile_key:
                return profile_key
    return default


def _configured_profile_name(config: Any) -> str:
    env_value = os.getenv("CANON_AGENT", "").strip()
    if env_value:
        return env_value
    for attr in ("canon_agent", "agent", "profile"):
        value = getattr(config, attr, None)
        if value:
            return str(value).strip()
    extra = getattr(config, "extra", {}) or {}
    if isinstance(extra, dict):
        for candidate in ("canon_agent", "agent", "profile", "CANON_AGENT"):
            value = extra.get(candidate)
            if value:
                return str(value).strip()
    return ""


def _agents_file_path() -> Path:
    override = os.getenv("CANON_AGENTS_FILE", "").strip()
    if override:
        return Path(override).expanduser()
    return Path.home() / ".canon" / "agents.json"


def _load_profile_api_key(profile_name: str) -> str:
    _bootstrap_agents_file()
    path = _agents_file_path()
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Could not read Canon profile file at {path}: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ValueError(f"Canon profile file at {path} must contain a JSON object")
    profile = data.get(profile_name)
    if not isinstance(profile, dict):
        return ""
    api_key = profile.get("apiKey") or profile.get("api_key")
    return str(api_key).strip() if api_key else ""


def _bootstrap_agents_file() -> None:
    raw = os.getenv("CANON_AGENTS_JSON_BOOTSTRAP", "").strip()
    if not raw:
        return
    bootstrap = _parse_agents_bootstrap(raw)
    path = _agents_file_path()
    existing: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Could not read Canon profile file at {path}: {exc}"
            ) from exc
        if not isinstance(loaded, dict):
            raise ValueError(f"Canon profile file at {path} must contain a JSON object")
        existing = loaded

    merged = dict(existing)
    changed = False
    for profile_name, profile in bootstrap.items():
        if profile_name not in merged:
            merged[profile_name] = profile
            changed = True
    if not changed and path.exists():
        return

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _write_json_0600(path, merged)
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        logger.debug("Could not chmod Canon profile file %s", path, exc_info=True)


def _write_json_0600(path: Path, data: dict[str, Any]) -> None:
    body = json.dumps(data, indent=2, sort_keys=True) + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    try:
        handle = os.fdopen(fd, "w", encoding="utf-8")
    except Exception:
        os.close(fd)
        raise
    with handle:
        handle.write(body)


def _parse_agents_bootstrap(raw: str) -> dict[str, Any]:
    candidates = [raw]
    try:
        decoded = base64.b64decode(raw, validate=True).decode("utf-8")
        candidates.append(decoded)
    except (binascii.Error, UnicodeDecodeError):
        pass

    last_error: Exception | None = None
    for candidate in candidates:
        try:
            data = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = exc
            continue
        if not isinstance(data, dict):
            raise ValueError("CANON_AGENTS_JSON_BOOTSTRAP must be a JSON object")
        for profile_name, profile in data.items():
            if not isinstance(profile_name, str) or not profile_name:
                raise ValueError("Canon bootstrap profile names must be non-empty strings")
            if not isinstance(profile, dict):
                raise ValueError("Canon bootstrap profiles must be JSON objects")
            if not (profile.get("apiKey") or profile.get("api_key")):
                raise ValueError("Canon bootstrap profiles must include apiKey")
        return data
    raise ValueError(
        "CANON_AGENTS_JSON_BOOTSTRAP must be raw JSON or base64-encoded JSON"
    ) from last_error


def _missing_api_key_message(config: Any) -> str:
    profile_name = _configured_profile_name(config)
    if profile_name:
        return (
            f"Canon profile '{profile_name}' was not found or has no apiKey. "
            "Set CANON_AGENTS_JSON_BOOTSTRAP, set CANON_API_KEY, or re-run Canon "
            "registration against the mounted volume."
        )
    return (
        "CANON_API_KEY/config.api_key is required for the Canon platform. "
        "Alternatively set CANON_AGENT with CANON_AGENTS_JSON_BOOTSTRAP or a "
        "mounted ~/.canon/agents.json profile."
    )


def _config_int(config: Any, key: str, env_name: str, default: int) -> int:
    raw = _config_value(config, key, env_name, "")
    if raw == "":
        return default
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return default


def _first_string(data: Any, *keys: str) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if value is not None and str(value) != "":
            return str(value)
    return None


def _conversation_name(conversation: Optional[dict[str, Any]]) -> Optional[str]:
    if not isinstance(conversation, dict):
        return None
    return _first_string(conversation, "name", "topic") or _first_string(
        conversation, "id"
    )


def _chat_type_from_conversation(conversation: Optional[dict[str, Any]]) -> str:
    if isinstance(conversation, dict) and conversation.get("type") == "group":
        return "group"
    return "dm"


def _message_text(message: dict[str, Any]) -> str:
    text = message.get("text")
    if isinstance(text, str) and text.strip():
        return text

    content_type = message.get("contentType")
    if content_type == "contact_card":
        card = message.get("contactCard")
        name = (
            _first_string(card, "displayName", "userId")
            if isinstance(card, dict)
            else None
        )
        return f"[Contact card: {name}]" if name else "[Contact card]"

    attachments = message.get("attachments")
    if isinstance(attachments, list) and attachments:
        labels: list[str] = []
        for item in attachments:
            if not isinstance(item, dict):
                continue
            kind = _first_string(item, "kind") or content_type or "file"
            name = _first_string(item, "fileName", "url")
            labels.append(
                f"{kind} attachment: {name}" if name else f"{kind} attachment"
            )
        if labels:
            return "[" + "; ".join(labels) + "]"

    if isinstance(content_type, str) and content_type != "text":
        return f"[{content_type} message]"
    return ""


def _message_type_for_text_and_media(
    text: str, media_type: Optional[MessageType]
) -> MessageType:
    if text.strip().startswith("/"):
        return MessageType.COMMAND
    return media_type or MessageType.TEXT


def _prefer_media_message_type(
    current: Optional[MessageType],
    candidate: MessageType,
) -> MessageType:
    priority = {
        MessageType.VOICE: 4,
        MessageType.AUDIO: 4,
        MessageType.VIDEO: 3,
        MessageType.PHOTO: 2,
        MessageType.DOCUMENT: 1,
    }
    if current is None:
        return candidate
    return (
        candidate if priority.get(candidate, 0) > priority.get(current, 0) else current
    )


def _guess_mime_type(path_or_name: str, override: Optional[str] = None) -> str:
    if override:
        return override
    guessed, _encoding = mimetypes.guess_type(path_or_name)
    if guessed:
        return guessed
    ext = Path(path_or_name.split("?", 1)[0]).suffix.lower()
    if ext in {".m4a"}:
        return "audio/mp4"
    if ext in {".opus"}:
        return "audio/ogg"
    if ext in VIDEO_EXTS:
        return "video/mp4"
    if ext in IMAGE_EXTS:
        return "image/jpeg"
    if ext in AUDIO_EXTS:
        return "audio/mpeg"
    return "application/octet-stream"


def _canon_attachment_kind_for_mime(mime_type: str) -> str:
    if mime_type.startswith("image/"):
        return "image"
    if mime_type.startswith("audio/"):
        return "audio"
    return "file"


def _attachment_mime_type(
    attachment: dict[str, Any],
    response_mime: Optional[str] = None,
) -> str:
    explicit = _first_string(attachment, "mimeType")
    if explicit:
        return explicit.split(";", 1)[0].strip().lower()
    if response_mime:
        return response_mime.split(";", 1)[0].strip().lower()
    name = _first_string(attachment, "fileName", "url") or ""
    return _guess_mime_type(name)


def _attachment_file_name(attachment: dict[str, Any], mime_type: str) -> str:
    name = _first_string(attachment, "fileName")
    if name:
        return Path(name).name

    url = _first_string(attachment, "url") or ""
    url_name = Path(url.split("?", 1)[0]).name
    if url_name and "." in url_name:
        return url_name

    ext = mimetypes.guess_extension(mime_type) or ".bin"
    return f"canon-media{ext}"


def _cache_canon_media(
    attachment: dict[str, Any],
    data: bytes,
    mime_type: str,
) -> tuple[str, MessageType]:
    ext = Path(_attachment_file_name(attachment, mime_type)).suffix.lower()
    if not ext:
        ext = mimetypes.guess_extension(mime_type) or ".bin"

    if mime_type.startswith("image/"):
        return cache_image_from_bytes(
            data, ext if ext in IMAGE_EXTS else ".jpg"
        ), MessageType.PHOTO
    if mime_type.startswith("audio/"):
        return cache_audio_from_bytes(
            data, ext if ext in AUDIO_EXTS else ".ogg"
        ), MessageType.VOICE
    if mime_type.startswith("video/"):
        return cache_video_from_bytes(
            data, ext if ext in VIDEO_EXTS else ".mp4"
        ), MessageType.VIDEO
    return cache_document_from_bytes(
        data, _attachment_file_name(attachment, mime_type)
    ), MessageType.DOCUMENT


def _redact_text(text: str) -> str:
    return SECRET_PATTERN.sub(lambda match: f"{match.group(1)}=[REDACTED]", str(text))


def _approval_risk_for_command(command: str) -> str:
    lowered = command.lower()
    destructive_fragments = (
        "rm -rf",
        "mkfs",
        "dd if=",
        "shutdown",
        "reboot",
        "poweroff",
        "chmod -r",
        "chown -r",
        "git reset --hard",
    )
    if any(fragment in lowered for fragment in destructive_fragments):
        return "destructive"
    high_risk_fragments = ("sudo", "curl", "wget", "ssh", "scp", "rsync")
    if any(fragment in lowered for fragment in high_risk_fragments):
        return "high"
    return "normal"


def _iso_after(seconds: int) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=seconds)
    ).isoformat().replace("+00:00", "Z")


def _ms_after(seconds: float) -> int:
    return int((time.time() + max(1.0, float(seconds))) * 1000)


def _iso_from_ms(milliseconds: int) -> str:
    return datetime.fromtimestamp(
        milliseconds / 1000.0,
        tz=timezone.utc,
    ).isoformat().replace("+00:00", "Z")


def _runtime_input_title(kind: str, secret_name: Optional[str] = None) -> str:
    if kind == "sudo":
        return "Sudo password required"
    if kind == "secret":
        return f"{secret_name} required" if secret_name else "Secret required"
    return "Input required"


def _approval_request_text(
    *,
    approval_id: str,
    command: str,
    description: str,
    expires_at: str,
) -> str:
    return "\n".join([
        f"Action Approval Required [{approval_id}]",
        "",
        "Tool: Shell",
        command,
        "",
        f"Reason: {description}",
        f"Expires at: {expires_at}",
    ])


def _approval_outcome_text(
    approval_id: str,
    command: str,
    decision: str,
    reason: str,
) -> str:
    label = "Approved" if decision == "allow" else "Denied"
    short = command if len(command) <= 60 else command[:60] + "..."
    if reason == "timeout":
        return f"Expired [{approval_id}] -- Shell: {short} (auto-denied)"
    return f"{label} [{approval_id}] -- Shell: {short}"


def _runtime_input_choices(choices: Optional[list]) -> list[dict[str, str]]:
    if not choices:
        return []
    normalized: list[dict[str, str]] = []
    for choice in choices[:12]:
        if isinstance(choice, dict):
            label = _first_string(choice, "label", "text", "value")
            value = _first_string(choice, "value")
            description = _first_string(choice, "description")
        else:
            label = str(choice).strip()
            value = label
            description = None
        if not label:
            continue
        item = {"label": label[:120]}
        if value:
            item["value"] = value[:200]
        if description:
            item["description"] = description[:300]
        normalized.append(item)
    return normalized


def _hermes_approval_choice(decision: str, session_rule: Any) -> str:
    if decision == "deny":
        return "deny"
    if isinstance(session_rule, dict):
        rule_type = session_rule.get("type")
        if rule_type == "deny-tool":
            return "deny"
        if rule_type in {"approve-all", "approve-tool"}:
            return "session"
    return "once"


def _media_file_path(item: Any) -> str:
    if isinstance(item, (list, tuple)) and item:
        return str(item[0])
    return str(item)


def _split_canon_metadata(
    metadata: Optional[Dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(metadata, dict):
        return {}, {}

    options = metadata.get("canon_options")
    canon_metadata = metadata.get("canon_metadata")

    safe_options = dict(options) if isinstance(options, dict) else {}
    safe_metadata = dict(canon_metadata) if isinstance(canon_metadata, dict) else {}

    if not safe_metadata and metadata:
        safe_metadata["hermes"] = metadata

    return safe_options, safe_metadata


def _safe_error(exc: Exception) -> str:
    if isinstance(exc, CanonStreamError):
        return f"Canon stream error {exc.code}: {exc.message}"
    if isinstance(exc, CanonApiError):
        return str(exc)
    return str(exc) or exc.__class__.__name__


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, CanonStreamError):
        return exc.retryable
    if isinstance(exc, CanonApiError):
        return exc.retryable
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
        ),
    )


def check_requirements() -> bool:
    """Canon uses Hermes' core httpx dependency, so imports are the only check."""
    return True


def validate_config(config) -> bool:
    return bool(_config_value(config, "api_key", "CANON_API_KEY"))


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    api_key = os.getenv("CANON_API_KEY", "").strip()
    profile_name = os.getenv("CANON_AGENT", "").strip()
    if not api_key and profile_name:
        api_key = _load_profile_api_key(profile_name)
    if not api_key:
        return None

    seed: dict[str, Any] = {"api_key": api_key}
    if profile_name:
        seed["canon_agent"] = profile_name
    if os.getenv("CANON_BASE_URL"):
        seed["base_url"] = os.getenv("CANON_BASE_URL", "").strip()
    if os.getenv("CANON_STREAM_URL"):
        seed["stream_url"] = os.getenv("CANON_STREAM_URL", "").strip()
    if os.getenv("CANON_HISTORY_LIMIT"):
        seed["history_limit"] = os.getenv("CANON_HISTORY_LIMIT", "").strip()
    if os.getenv("CANON_HOME_CHANNEL"):
        seed["home_channel"] = {
            "chat_id": os.getenv("CANON_HOME_CHANNEL", "").strip(),
            "name": os.getenv("CANON_HOME_CHANNEL_NAME", "Canon Home"),
        }
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id=None,
    media_files=None,
    force_document: bool = False,
) -> dict:
    api_key = _config_value(pconfig, "api_key", "CANON_API_KEY")
    if not api_key:
        return {
            "error": "Canon standalone send failed: CANON_API_KEY is not configured"
        }

    target = chat_id or os.getenv("CANON_HOME_CHANNEL", "").strip()
    if not target:
        home = getattr(pconfig, "home_channel", None)
        target = getattr(home, "chat_id", "") or ""
    if not target:
        return {"error": "Canon standalone send failed: no conversation ID provided"}

    text = message or ""

    client = CanonHttpClient(
        api_key,
        base_url=_config_value(pconfig, "base_url", "CANON_BASE_URL", DEFAULT_BASE_URL),
        stream_url=_config_value(
            pconfig, "stream_url", "CANON_STREAM_URL", DEFAULT_STREAM_URL
        ),
    )
    try:
        options: dict[str, Any] = {}
        if media_files:
            attachments: list[dict[str, Any]] = []
            for item in media_files:
                media_path = _media_file_path(item)
                path = Path(media_path).expanduser()
                if not path.exists():
                    return {
                        "error": f"Canon standalone send failed: media file not found: {media_path}"
                    }
                if path.stat().st_size > MAX_MEDIA_BYTES:
                    return {
                        "error": "Canon standalone send failed: Canon media upload limit is 10MB"
                    }
                mime_type = _guess_mime_type(str(path))
                uploaded = await client.upload_media(
                    target,
                    base64.b64encode(path.read_bytes()).decode("ascii"),
                    mime_type,
                    file_name=path.name,
                )
                attachment = dict(uploaded.get("attachment") or {})
                if not attachment:
                    attachment = {
                        "kind": _canon_attachment_kind_for_mime(mime_type),
                        "url": uploaded.get("url"),
                        "mimeType": mime_type,
                        "fileName": path.name,
                        "sizeBytes": path.stat().st_size,
                    }
                attachments.append(attachment)

            if attachments:
                first_kind = str(attachments[0].get("kind") or "file")
                options["contentType"] = (
                    first_kind if first_kind in {"image", "audio"} else "file"
                )
                options["attachments"] = attachments

        data = await client.send_message(
            target,
            text,
            reply_to=str(thread_id) if thread_id else None,
            metadata=dict(TURN_COMPLETE_METADATA),
            options=options or None,
        )
        return {"success": True, "message_id": _first_string(data, "messageId", "id")}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return {"error": f"Canon standalone send failed: {_safe_error(exc)}"}
    finally:
        await client.close()


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="canon",
        label="Canon",
        adapter_factory=lambda cfg: CanonAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["CANON_API_KEY"],
        install_hint="Set CANON_API_KEY for your Canon agent",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="CANON_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="CANON_ALLOWED_USERS",
        allow_all_env="CANON_ALLOW_ALL_USERS",
        max_message_length=8000,
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Canon. Canon conversations can be direct or group chats. "
            "Use concise conversational text; slash commands are preserved when users send them. "
            "Messages sent by Hermes are marked as turn-complete for Canon's UI."
        ),
    )
