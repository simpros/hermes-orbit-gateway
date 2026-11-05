"""Orbit chat platform adapter for Hermes Agent.

Orbit (https://github.com/coollabsio/orbit) authenticates chat over a *session*:
``POST /api/v1/auth/login`` hands back a cookie (or a bearer token for native
clients) and every chat route — plus the live websocket — is judged by it.  There
is no API-token path for chat writes, so this adapter owns a session, re-logs in
when it expires, and speaks the same REST API the web client uses.

Config (``gateway.platforms.orbit.extra`` or env, env wins):

======================  ==========================================================
``ORBIT_BASE_URL``      Orbit instance base URL (required)
``ORBIT_WORKSPACE_ID``  workspace the bot lives in (required)
``ORBIT_EMAIL``         account email — with ``ORBIT_PASSWORD`` performs a login
``ORBIT_PASSWORD``      account password
``ORBIT_SESSION_COOKIE``      pre-issued cookie (``name=value``) or bare token
``ORBIT_SESSION_COOKIE_NAME`` cookie name for a bare token
``ORBIT_HOME_CHANNEL``  conversation id used by cron / ``deliver=orbit``
``ORBIT_*``             see ``plugin.yaml`` for the full table
======================  ==========================================================

Transport: polling is the source of truth.  ``GET .../conversations/{id}/messages``
with an ``after`` cursor per conversation, ``GET .../threads/{root}`` for threads
the bot is part of.  The ``/live`` websocket (if reachable) is a best-effort
low-latency hint: it never advances the poll cursors, so a dropped socket costs
latency and nothing else.  Both paths feed one deduplicating dispatcher.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import mimetypes
import random
import re
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from gateway.config import Platform
from gateway.platforms._shared import (
    apply_yaml_bridge,
    extra_or_secret,
    get_scoped_secret,
    seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult, classify_send_error
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator, cancel_task

logger = logging.getLogger(__name__)

PLATFORM_NAME = "orbit"
DEFAULT_TIMEOUT = 30.0
# Orbit rejects a message body longer than 4000 characters.
MAX_BODY_CHARS = 4000
DEFAULT_POLL_INTERVAL = 3.0
CONVERSATION_REFRESH_SECONDS = 30.0
MAX_WATCHED_THREADS = 200
MAX_POLL_PAGE = 100
# Secure instances name the session cookie ``__Host-``-prefixed; a loopback
# development server uses an insecure name so plain HTTP works.
SECURE_COOKIE_NAME = "__Host-orbit_session"
DEV_COOKIE_NAME = "orbit_session_dev"
_TRUTHY = {"1", "true", "yes", "on"}
_USER_AGENT = "hermes-orbit-gateway/0.1 (+https://github.com/simpros/hermes-orbit-gateway)"

# ``<@uuid>`` / ``<#uuid>`` tokens are how Orbit stores mentions in a body.
_MENTION_TOKEN_RE = re.compile(r"<@([^<>\s]+)>")


def _truthy(value: Any, default: bool) -> bool:
    """Lenient bool for env/YAML values (``"false"`` must not read as True)."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUTHY


def _coerce_float(value: Any, default: float, *, minimum: float = 0.5) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= minimum else default


def _csv_set(value: Any) -> set[str]:
    """Comma-separated ids from env/YAML (a YAML list is accepted as-is)."""
    if isinstance(value, (list, tuple, set)):
        items = [str(item) for item in value]
    else:
        items = str(value or "").split(",")
    return {item.strip() for item in items if item.strip()}


def _origin_of(base_url: str) -> str:
    """Scheme://host[:port] of a base URL — Orbit's CSRF policy compares this."""
    parts = urlsplit(base_url)
    if not parts.scheme or not parts.netloc:
        return ""
    return f"{parts.scheme}://{parts.netloc}"


def _is_loopback(base_url: str) -> bool:
    host = (urlsplit(base_url).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1", "[::1]"}


def _ws_url(base_url: str, workspace_id: str) -> str:
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme == "https" else "ws"
    root = parts.path.rstrip("/")
    return f"{scheme}://{parts.netloc}{root}/api/v1/workspaces/{workspace_id}/live"


def _parse_timestamp(value: Any) -> datetime:
    """Orbit timestamps are ISO-8601 strings with millisecond precision."""
    if isinstance(value, (int, float)):
        with contextlib.suppress(ValueError, OSError):
            return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
        return datetime.now()
    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        with contextlib.suppress(ValueError):
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now()


def _split_cookie(raw: str) -> tuple[str, str]:
    """Split ``name=value`` from a bare token; ``("", token)`` for a bare token."""
    value = (raw or "").strip()
    if not value:
        return "", ""
    if ";" in value:  # a full Cookie header — keep only its first pair
        value = value.split(";", 1)[0].strip()
    name, sep, token = value.partition("=")
    if sep and name and " " not in name:
        return name.strip(), token.strip()
    return "", value


def _message_sort_key(message: dict) -> str:
    """Orbit ids are UUIDv7: lexical order is creation order."""
    return str(message.get("id") or "")


def _root_signature(root: dict) -> tuple:
    """Cheap change detector for a root's thread summary."""
    return (int(root.get("reply_count") or 0), str(root.get("last_reply_at") or ""))


class OrbitAPIError(RuntimeError):
    """A non-2xx response (or an unusable body) from the Orbit API."""

    def __init__(self, message: str, *, status: int | None = None, code: str = ""):
        super().__init__(message)
        self.status = status
        self.code = code

    @property
    def unauthorized(self) -> bool:
        return self.status == 401


class OrbitAuthError(OrbitAPIError):
    """Sign-in failed and the adapter cannot continue without new credentials."""


class OrbitAPI:
    """Thin async client for the Orbit chat REST API with session management.

    Only the routes the gateway needs are wrapped: conversations, messages,
    threads, file upload, members.  Every request carries an ``Origin`` header
    because Orbit's CSRF policy rejects state-changing requests without one, and
    a session cookie (or bearer token) because chat is session-only.
    """

    def __init__(
        self,
        base_url: str,
        workspace_id: str,
        *,
        email: str = "",
        password: str = "",
        session_cookie: str = "",
        cookie_name: str = "",
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.base_url = (base_url or "").rstrip("/")
        self.workspace_id = (workspace_id or "").strip()
        self.email = (email or "").strip()
        self.password = password or ""
        self.origin = _origin_of(self.base_url)
        name, token = _split_cookie(session_cookie)
        self._cookie_name = (cookie_name or name or "").strip()
        self._seeded_token = token
        self._cookie = ""
        self._bearer = ""
        if token:
            if self._cookie_name:
                self._cookie = f"{self._cookie_name}={token}"
            else:  # a bare token: present it as a bearer session
                self._bearer = token
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._login_lock = asyncio.Lock()
        self._has_session = bool(self._cookie or self._bearer)

    # ── transport ───────────────────────────────────────────────────────────

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                trust_env=False,
                follow_redirects=True,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            with contextlib.suppress(Exception):
                await self._client.aclose()
        self._client = None

    def session_headers(self) -> dict[str, str]:
        """Headers that carry the session (cookie wins over bearer, as in Orbit)."""
        if self._cookie:
            return {"Cookie": self._cookie}
        if self._bearer:
            return {"Authorization": f"Bearer {self._bearer}"}
        return {}

    def _path(self, suffix: str) -> str:
        return f"/api/v1/workspaces/{self.workspace_id}/chat{suffix}"

    async def login(self, *, force: bool = False) -> None:
        """Sign in with email/password and keep the session cookie (idempotent)."""
        async with self._login_lock:
            if self._has_session and not force:
                return
            if not (self.email and self.password):
                # Nothing to log in with: keep a pre-issued cookie if we have one.
                if self._cookie or self._bearer:
                    return
                raise OrbitAuthError(
                    "Orbit: no session and no credentials — set ORBIT_SESSION_COOKIE "
                    "or ORBIT_EMAIL + ORBIT_PASSWORD"
                )
            client = await self._http()
            try:
                response = await client.post(
                    "/api/v1/auth/login",
                    json={"email": self.email, "password": self.password},
                    # Origin makes Orbit answer with the cookie delivery mode.
                    headers={"Origin": self.origin, "Content-Type": "application/json"},
                )
            except httpx.HTTPError as exc:
                raise OrbitAPIError(f"Orbit login transport error: {exc}") from exc
            if response.status_code == 202:
                raise OrbitAuthError(
                    "Orbit: the account has two-factor sign-in enabled; "
                    "use ORBIT_SESSION_COOKIE with a session issued elsewhere"
                )
            if response.status_code != 200:
                detail = _problem_detail(response)
                raise OrbitAuthError(
                    f"Orbit login failed ({response.status_code}): {detail}",
                    status=response.status_code,
                )
            cookies = response.headers.get_list("set-cookie")
            pairs = [c.split(";", 1)[0].strip() for c in cookies if c.split(";", 1)[0].strip()]
            if pairs:
                self._cookie = "; ".join(pairs)
                self._cookie_name = pairs[0].split("=", 1)[0]
                self._bearer = ""
            else:
                payload = _json_payload(response)
                token = str(payload.get("session_token") or "") if isinstance(payload, dict) else ""
                if not token:
                    raise OrbitAuthError("Orbit login returned neither a cookie nor a session token")
                self._bearer = token
                self._cookie = ""
            self._has_session = True
            self._seeded_token = ""
            logger.info("Orbit: signed in as %s", self.email)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: Any = None,
        files: Any = None,
        expect: tuple[int, ...] = (200, 201),
    ) -> Any:
        """Authenticated request with one automatic re-login + retry on 401."""
        await self.login()
        for attempt in (0, 1):
            headers = {"Origin": self.origin, **self.session_headers()}
            try:
                response = await (await self._http()).request(
                    method, path, params=params, json=json_body, files=files, headers=headers
                )
            except httpx.HTTPError as exc:
                raise OrbitAPIError(f"Orbit {method} {path} transport error: {exc}") from exc
            if response.status_code == 401 and attempt == 0:
                logger.info("Orbit: session expired on %s %s — signing in again", method, path)
                self._has_session = False
                with contextlib.suppress(OrbitAPIError):
                    await self.login(force=True)
                if not (self._cookie or self._bearer):
                    raise OrbitAuthError(
                        f"Orbit {method} {path}: session rejected and no credentials to renew it",
                        status=401,
                    )
                continue
            if response.status_code not in expect:
                raise OrbitAPIError(
                    f"Orbit {method} {path} -> {response.status_code}: {_problem_detail(response)}",
                    status=response.status_code,
                    code=_problem_code(response),
                )
            if response.status_code == 204 or not response.content:
                return {}
            return _json_payload(response)
        raise OrbitAPIError(f"Orbit {method} {path}: unauthorized after re-login", status=401)

    # ── routes ──────────────────────────────────────────────────────────────

    async def me(self) -> dict:
        return await self._request("GET", "/api/v1/auth/me", expect=(200,))

    async def list_conversations(self) -> list[dict]:
        data = await self._request("GET", self._path("/conversations"), expect=(200,))
        return data if isinstance(data, list) else []

    async def list_members(self, *, limit: int = 200) -> list[dict]:
        data = await self._request(
            "GET", f"/api/v1/workspaces/{self.workspace_id}/members",
            params={"limit": limit}, expect=(200,),
        )
        if isinstance(data, dict):
            return list(data.get("items") or [])
        return data if isinstance(data, list) else []

    async def list_messages(
        self, conversation_id: str, *, after: str | None = None,
        before: str | None = None, limit: int = 50,
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": max(1, min(limit, MAX_POLL_PAGE))}
        if after:
            params["after"] = after
        elif before:
            params["before"] = before
        data = await self._request(
            "GET", self._path(f"/conversations/{conversation_id}/messages"),
            params=params, expect=(200,),
        )
        return list((data or {}).get("items") or [])

    async def get_thread(
        self, root_id: str, *, after: str | None = None, limit: int = 50,
    ) -> dict:
        params: dict[str, Any] = {"limit": max(1, min(limit, MAX_POLL_PAGE))}
        if after:
            params["after"] = after
        return await self._request(
            "GET", self._path(f"/threads/{root_id}"), params=params, expect=(200,)
        )

    async def list_followed_threads(self) -> list[dict]:
        data = await self._request("GET", self._path("/threads"), expect=(200,))
        items = data.get("items") if isinstance(data, dict) else data
        return list(items or [])

    async def open_dm(self, user_ids: list[str]) -> dict:
        data = await self._request(
            "POST", self._path("/dms"), json_body={"user_ids": user_ids}, expect=(200, 201)
        )
        return dict((data or {}).get("result") or {})

    async def send_message(
        self,
        conversation_id: str,
        body: str,
        *,
        thread_root_id: str | None = None,
        reply_to_id: str | None = None,
        file_ids: list[str] | None = None,
        nonce: str | None = None,
        also_in_channel: bool = False,
    ) -> dict:
        payload: dict[str, Any] = {
            "body": body,
            "nonce": nonce or uuid.uuid4().hex,
            "file_ids": list(file_ids or []),
            "also_in_channel": bool(also_in_channel),
        }
        if thread_root_id:
            payload["thread_root_id"] = thread_root_id
        if reply_to_id:
            payload["reply_to_id"] = reply_to_id
        data = await self._request(
            "POST", self._path(f"/conversations/{conversation_id}/messages"),
            json_body=payload, expect=(200, 201),
        )
        return dict((data or {}).get("result") or {})

    async def upload_file(self, path: str, *, mime_type: str = "") -> dict:
        file_path = Path(path)
        if not file_path.is_file():
            raise OrbitAPIError(f"Orbit upload: {path} is not a readable file")
        guessed = mime_type or mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        with file_path.open("rb") as handle:
            files = {"file": (file_path.name, handle.read(), guessed)}
        data = await self._request(
            "POST", self._path("/files"), files=files, expect=(200, 201)
        )
        return dict(data or {})

    # ── presence (websocket only) ───────────────────────────────────────────

    async def open_live(self, *, epoch: str | None, after: int | None, timeout: float = 15.0):
        """Open the live websocket; the caller owns the connection."""
        import websockets  # optional: polling works without it

        url = _ws_url(self.base_url, self.workspace_id)
        query = []
        if epoch:
            query.append(f"epoch={epoch}")
        if after is not None:
            query.append(f"after={int(after)}")
        if query:
            url = f"{url}?{'&'.join(query)}"
        headers = {"Origin": self.origin, **self.session_headers()}
        try:
            return await websockets.connect(
                url, additional_headers=headers, open_timeout=timeout,
                ping_interval=20, ping_timeout=20, max_size=4 * 1024 * 1024,
            )
        except TypeError:  # websockets < 14 spells the argument differently
            return await websockets.connect(
                url, extra_headers=headers, open_timeout=timeout,
                ping_interval=20, ping_timeout=20, max_size=4 * 1024 * 1024,
            )


def _json_payload(response: httpx.Response) -> Any:
    """Decoded JSON body (an object for most routes, a list for conversation/member lists)."""
    with contextlib.suppress(ValueError):
        return response.json()
    return {}


def _problem_code(response: httpx.Response) -> str:
    with contextlib.suppress(ValueError):
        return str(response.json().get("code") or "")
    return ""


def _problem_detail(response: httpx.Response) -> str:
    with contextlib.suppress(ValueError):
        payload = response.json()
        if isinstance(payload, dict):
            return str(payload.get("detail") or payload.get("title") or payload)
    return (response.text or "").strip()[:300] or "no detail"


# ── Adapter ─────────────────────────────────────────────────────────────────


class OrbitAdapter(BasePlatformAdapter):
    """Hermes adapter for Orbit chat: channels, threads and DMs."""

    MAX_MESSAGE_LENGTH = MAX_BODY_CHARS
    # send() chunks long answers through truncate_message (4000-char body cap).
    splits_long_messages = True
    # Orbit renders Markdown (bold, code fences) in the message body.
    supports_code_blocks = True

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=_orbit_platform())
        extra = getattr(config, "extra", {}) or {}
        self.base_url = str(extra_or_secret(extra, "base_url", "ORBIT_BASE_URL", "")).strip()
        self.workspace_id = str(extra_or_secret(extra, "workspace_id", "ORBIT_WORKSPACE_ID", "")).strip()
        self.email = str(extra_or_secret(extra, "email", "ORBIT_EMAIL", "")).strip()
        self.password = str(extra_or_secret(extra, "password", "ORBIT_PASSWORD", ""))
        self.session_cookie = str(extra_or_secret(extra, "session_cookie", "ORBIT_SESSION_COOKIE", ""))
        self.session_cookie_name = str(
            extra_or_secret(extra, "session_cookie_name", "ORBIT_SESSION_COOKIE_NAME", "")
        ).strip()
        self.poll_interval = _coerce_float(
            extra_or_secret(extra, "poll_interval", "ORBIT_POLL_INTERVAL", DEFAULT_POLL_INTERVAL),
            DEFAULT_POLL_INTERVAL,
        )
        self.use_websocket = _truthy(
            extra_or_secret(extra, "use_websocket", "ORBIT_USE_WEBSOCKET", None), True
        )
        self.require_mention = _truthy(
            extra_or_secret(extra, "require_mention", "ORBIT_REQUIRE_MENTION", None), True
        )
        self.mention_name = str(
            extra_or_secret(extra, "mention_name", "ORBIT_MENTION_NAME", "")
        ).strip()
        # Discord parity: a channel mention opens its own thread instead of answering inline.
        self.auto_thread = _truthy(
            extra_or_secret(extra, "auto_thread", "ORBIT_AUTO_THREAD", None), True
        )
        # Conversations that keep answering inline (Discord's ``no_thread_channels``).
        self.no_thread_channels = _csv_set(
            extra_or_secret(extra, "no_thread_channels", "ORBIT_NO_THREAD_CHANNELS", "")
        )
        self.home_channel = str(
            extra_or_secret(extra, "home_channel", "ORBIT_HOME_CHANNEL", "")
        ).strip()

        self.api = OrbitAPI(
            self.base_url,
            self.workspace_id,
            email=self.email,
            password=self.password,
            session_cookie=self.session_cookie,
            cookie_name=self.session_cookie_name,
        )

        self._self_id = ""
        self._self_name = ""
        self._conversations: dict[str, dict] = {}
        self._member_names: dict[str, str] = {}
        # conv_id -> last main-list message id the poller has consumed
        self._cursors: dict[str, str] = {}
        self._seeded: set[str] = set()
        # root message id -> conversation id, and root -> last reply id consumed
        self._thread_roots: dict[str, str] = {}
        self._thread_cursors: dict[str, str] = {}
        # (reply_count, last_reply_at): only a moved summary is worth a thread request.
        self._thread_signature: dict[str, tuple] = {}
        self._active_threads: set[str] = set()
        # Threads the bot has itself replied in: replies there address it implicitly.
        self._bot_threads: set[str] = set()
        self._dedup = MessageDeduplicator()
        self._poll_task: asyncio.Task | None = None
        self._live_task: asyncio.Task | None = None
        self._ws = None
        self._live_epoch: str | None = None
        self._live_seq: int | None = None
        self._last_conversation_refresh = 0.0
        self._reconnect_delay = 1.0

    # ── lifecycle ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "Orbit"

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not (self.base_url and self.workspace_id):
            logger.error("Orbit: ORBIT_BASE_URL and ORBIT_WORKSPACE_ID must be configured")
            return self._fail("config_missing", "ORBIT_BASE_URL and ORBIT_WORKSPACE_ID must be set", retryable=False)
        if not (self.session_cookie or (self.email and self.password)):
            logger.error("Orbit: need ORBIT_SESSION_COOKIE or ORBIT_EMAIL + ORBIT_PASSWORD")
            return self._fail(
                "config_missing", "Orbit needs a session cookie or email/password", retryable=False
            )
        if not self._acquire_platform_lock(
            PLATFORM_NAME, f"{self.base_url}|{self.workspace_id}|{self.email or 'cookie'}",
            f"Orbit session for {self.email or self.base_url}",
        ):
            return False
        try:
            await self.api.login()
            me = await self.api.me()
        except OrbitAPIError as exc:
            logger.error("Orbit: sign-in failed: %s", exc)
            self._release_platform_lock()
            return self._fail("auth_failed", str(exc), retryable=not isinstance(exc, OrbitAuthError))

        self._self_id = str(me.get("id") or "")
        self._self_name = str(me.get("display_name") or "")
        await self._refresh_conversations()
        self._mark_connected()
        logger.info(
            "Orbit: connected to %s workspace %s as %s (%s), %d conversations",
            self.base_url, self.workspace_id, self._self_name or "?", self._self_id,
            len(self._conversations),
        )
        self._poll_task = asyncio.create_task(self._poll_loop(), name="orbit-poll")
        if self.use_websocket:
            self._live_task = asyncio.create_task(self._live_loop(), name="orbit-live")
        self._wire_plugin_handlers(None)
        return True

    async def disconnect(self) -> None:
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()
        await cancel_task(self._live_task)
        await cancel_task(self._poll_task)
        self._live_task = self._poll_task = None
        await self._close_ws()
        await self.api.close()

    async def _close_ws(self) -> None:
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None

    # ── discovery / mapping ─────────────────────────────────────────────────

    async def _refresh_conversations(self) -> None:
        conversations = await self.api.list_conversations()
        self._conversations = {str(c.get("id")): c for c in conversations if c.get("id")}
        self._last_conversation_refresh = asyncio.get_running_loop().time()
        # A DM title is assembled from the other members' names.
        try:
            self._member_names = {
                str(m.get("user_id") or m.get("id")): str(m.get("display_name") or "")
                for m in await self.api.list_members()
            }
        except OrbitAPIError as exc:
            logger.debug("Orbit: member directory unavailable: %s", exc)
        logger.debug("Orbit: %d conversations visible", len(self._conversations))

    def _conversation_type(self, conversation: dict | None) -> str:
        return "dm" if (conversation or {}).get("kind") == "dm" else "group"

    def _conversation_name(self, conversation: dict | None) -> str:
        conversation = conversation or {}
        name = str(conversation.get("name") or "").strip()
        if name:
            return name
        if conversation.get("kind") == "dm":
            others = [
                self._member_names.get(str(uid), str(uid))
                for uid in conversation.get("member_ids") or []
                if str(uid) != self._self_id
            ]
            if len(others) == 1:
                return f"DM with {others[0]}"
            if others:
                return "DM with " + ", ".join(others)
            if conversation.get("self_dm"):
                return "Self DM"
            return "Direct message"
        return str(conversation.get("id") or "")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        conversation = self._conversations.get(str(chat_id))
        if conversation is None:
            with contextlib.suppress(OrbitAPIError):
                await self._refresh_conversations()
            conversation = self._conversations.get(str(chat_id))
        return {
            "name": self._conversation_name(conversation),
            "type": self._conversation_type(conversation),
            "chat_id": str(chat_id),
        }

    # ── outbound ────────────────────────────────────────────────────────────

    @staticmethod
    def _thread_from(metadata: dict | None) -> str | None:
        thread_id = (metadata or {}).get("thread_id")
        return str(thread_id) if thread_id else None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """Send Markdown to a conversation, or into a thread when ``metadata`` says so."""
        if not (content or "").strip():
            return SendResult(success=False, error="Refusing to send empty message")
        thread_root = self._thread_from(metadata)
        chunks = self.truncate_message(content, self.MAX_MESSAGE_LENGTH)
        sent: list[str] = []
        for index, chunk in enumerate(chunks):
            try:
                message = await self.api.send_message(
                    str(chat_id),
                    chunk,
                    thread_root_id=thread_root,
                    # Orbit allows an inline quote and a thread root; quote only outside threads.
                    reply_to_id=(reply_to if index == 0 and not thread_root else None),
                )
            except OrbitAPIError as exc:
                logger.warning("Orbit: send to %s failed: %s", chat_id, exc)
                if sent:
                    return SendResult(
                        success=False,
                        message_id=sent[-1],
                        continuation_message_ids=tuple(sent[:-1]),
                        error=str(exc),
                        error_kind=classify_send_error(None, str(exc)),
                        retryable=exc.status in (None, 429) or (exc.status or 0) >= 500,
                    )
                return SendResult(
                    success=False,
                    error=str(exc),
                    error_kind=classify_send_error(None, str(exc)),
                    retryable=exc.status in (None, 429) or (exc.status or 0) >= 500,
                )
            sent.append(str(message.get("id") or ""))
        if thread_root:
            self._bot_threads.add(thread_root)
            self._thread_roots.setdefault(thread_root, str(chat_id))
            self._thread_cursors.setdefault(thread_root, sent[-1] if sent else thread_root)
        return SendResult(
            success=True,
            message_id=sent[-1] if sent else None,
            continuation_message_ids=tuple(sent[:-1]),
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Orbit typing is a live-socket frame; silently skipped without a socket."""
        if self._ws is None:
            return
        frame = {"type": "typing", "conversation_id": str(chat_id)}
        thread_root = self._thread_from(metadata)
        if thread_root:
            frame["thread_root_id"] = thread_root
        with contextlib.suppress(Exception):
            await self._ws.send(json.dumps(frame))

    async def _send_upload(
        self, chat_id: str, path: str, caption: str | None,
        reply_to: str | None, metadata: dict[str, Any] | None,
    ) -> SendResult:
        try:
            uploaded = await self.api.upload_file(path)
        except OrbitAPIError as exc:
            return SendResult(success=False, error=str(exc))
        file_id = str(uploaded.get("id") or "")
        if not file_id:
            return SendResult(success=False, error="Orbit upload returned no file id")
        try:
            message = await self.api.send_message(
                str(chat_id),
                caption or "",
                thread_root_id=self._thread_from(metadata),
                reply_to_id=reply_to if not self._thread_from(metadata) else None,
                file_ids=[file_id],
            )
        except OrbitAPIError as exc:
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=str(message.get("id") or ""))

    async def send_image(
        self, chat_id: str, image_url: str, caption: str | None = None,
        reply_to: str | None = None, metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        if str(image_url).startswith(("http://", "https://")):
            try:
                async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
                    response = await client.get(str(image_url))
                    response.raise_for_status()
                    suffix = mimetypes.guess_extension(
                        response.headers.get("content-type", "").split(";")[0]
                    ) or ".png"
                    payload = response.content
            except Exception as exc:  # noqa: BLE001 - fall back to a link, never lose the reply
                logger.debug("Orbit: image download failed (%s), sending the URL", exc)
                text = f"{caption}\n{image_url}" if caption else str(image_url)
                return await self.send(chat_id, text, reply_to=reply_to, metadata=metadata)
            handle = tempfile.NamedTemporaryFile(suffix=suffix, prefix="orbit-image-", delete=False)
            try:
                handle.write(payload)
                handle.close()
                return await self._send_upload(chat_id, handle.name, caption, reply_to, metadata)
            finally:
                with contextlib.suppress(OSError):
                    Path(handle.name).unlink()
        return await self._send_upload(chat_id, str(image_url), caption, reply_to, metadata)

    async def send_image_file(
        self, chat_id: str, image_path: str, caption: str | None = None,
        reply_to: str | None = None, metadata: dict[str, Any] | None = None, **kwargs,
    ) -> SendResult:
        return await self._send_upload(chat_id, str(image_path), caption, reply_to, metadata)

    async def send_document(
        self, chat_id: str, file_path: str, caption: str | None = None,
        file_name: str | None = None, reply_to: str | None = None,
        metadata: dict[str, Any] | None = None, **kwargs,
    ) -> SendResult:
        return await self._send_upload(chat_id, str(file_path), caption, reply_to, metadata)

    # ── polling (source of truth) ────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        failures = 0
        while True:
            try:
                await self._poll_once()
                failures = 0
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                raise
            except OrbitAPIError as exc:
                failures += 1
                logger.warning("Orbit: poll failed (%s), retrying", exc)
                await asyncio.sleep(min(60.0, self.poll_interval * (2 ** min(failures, 5))))
            except Exception:  # noqa: BLE001 - a poll bug must not kill the adapter
                failures += 1
                logger.exception("Orbit: unexpected poll error, retrying")
                await asyncio.sleep(min(60.0, self.poll_interval * (2 ** min(failures, 5))))

    async def _poll_once(self) -> None:
        now = asyncio.get_running_loop().time()
        if not self._conversations or now - self._last_conversation_refresh > CONVERSATION_REFRESH_SECONDS:
            await self._refresh_conversations()
            await self._scan_thread_summaries()
        for conversation_id in list(self._conversations):
            try:
                await self._poll_conversation(conversation_id)
            except OrbitAPIError as exc:
                if exc.status in (403, 404):  # left, archived, or not a member
                    logger.debug("Orbit: conversation %s not readable: %s", conversation_id, exc)
                    self._cursors.pop(conversation_id, None)
                    self._seeded.discard(conversation_id)
                    continue
                raise
        await self._poll_threads()

    async def _poll_conversation(self, conversation_id: str) -> None:
        conversation = self._conversations.get(conversation_id) or {}
        if conversation_id not in self._seeded:
            await self._seed_conversation(conversation_id)
            return
        cursor = self._cursors.get(conversation_id)
        messages = await self.api.list_messages(conversation_id, after=cursor, limit=MAX_POLL_PAGE)
        for message in sorted(messages, key=_message_sort_key):
            if message.get("thread_root_id") is None:
                await self._prime_thread(message)
            await self._dispatch(message, conversation)
        if messages:
            self._cursors[conversation_id] = max(_message_sort_key(m) for m in messages)

    async def _seed_conversation(self, conversation_id: str) -> None:
        """First sight: learn the head of a conversation without replaying its history."""
        self._seeded.add(conversation_id)
        messages = sorted(
            await self.api.list_messages(conversation_id, limit=50), key=_message_sort_key
        )
        if messages:
            self._cursors[conversation_id] = _message_sort_key(messages[-1])
            for message in messages:
                if message.get("thread_root_id") is None:
                    await self._prime_thread(message, startup=True)

    async def _prime_thread(self, root: dict, *, startup: bool = False) -> None:
        """Watch a root's replies without replaying the ones that existed before startup."""
        root_id = str(root.get("id") or "")
        if not root_id:
            return
        self._thread_signature[root_id] = _root_signature(root)
        if root_id in self._thread_roots:
            return
        self._thread_roots[root_id] = str(root.get("conversation_id") or "")
        # Ids are UUIDv7, so the root id is a safe floor: every reply sorts above it.
        self._thread_cursors[root_id] = root_id
        if startup and (root.get("reply_count") or root.get("last_reply_at")):
            # Pre-existing thread on the very first run: start at its newest reply.
            with contextlib.suppress(OrbitAPIError):
                page = await self.api.get_thread(root_id, limit=MAX_POLL_PAGE)
                replies = sorted(page.get("items") or [], key=_message_sort_key)
                if replies:
                    self._thread_cursors[root_id] = _message_sort_key(replies[-1])
        self._trim_threads()

    def _trim_threads(self) -> None:
        while len(self._thread_roots) > MAX_WATCHED_THREADS:
            oldest = next(iter(self._thread_roots))
            self._thread_roots.pop(oldest, None)
            self._thread_cursors.pop(oldest, None)
            self._thread_signature.pop(oldest, None)
            self._active_threads.discard(oldest)

    async def _scan_thread_summaries(self) -> None:
        """Head scan: notice roots whose reply summary moved in threads we do not follow."""
        for conversation_id in list(self._conversations):
            try:
                messages = await self.api.list_messages(conversation_id, limit=50)
            except OrbitAPIError as exc:
                if exc.status not in (403, 404):
                    logger.debug("Orbit: head scan of %s failed: %s", conversation_id, exc)
                continue
            for message in messages:
                if message.get("thread_root_id") is not None:
                    continue
                root_id = str(message.get("id") or "")
                if not root_id:
                    continue
                previous = self._thread_signature.get(root_id)
                if previous is None:
                    await self._prime_thread(message, startup=True)
                    continue
                signature = _root_signature(message)
                self._thread_signature[root_id] = signature
                if signature != previous and message.get("reply_count"):
                    self._active_threads.add(root_id)

    async def _poll_threads(self) -> None:
        """Fetch replies only for threads the bot follows or whose summary moved."""
        try:
            followed = await self.api.list_followed_threads()
        except OrbitAPIError as exc:
            logger.debug("Orbit: followed threads unavailable: %s", exc)
            followed = []
        for record in followed:
            root = record.get("root") or {}
            root_id = str(root.get("id") or "")
            if not root_id:
                continue
            if root_id not in self._thread_roots:
                self._thread_roots[root_id] = str(
                    root.get("conversation_id") or record.get("conversation_id") or ""
                )
                last = (record.get("last_reply") or {}).get("id")
                self._thread_cursors[root_id] = str(last) if last else root_id
            # Orbit only lists threads the caller follows, i.e. has taken part in.
            self._bot_threads.add(root_id)
            if (record.get("state") or {}).get("unread_replies"):
                self._active_threads.add(root_id)
            elif str((record.get("last_reply") or {}).get("id") or "") not in (
                "", self._thread_cursors.get(root_id)
            ):
                self._active_threads.add(root_id)
        self._trim_threads()

        active, self._active_threads = self._active_threads, set()
        for root_id in active:
            conversation_id = self._thread_roots.get(root_id)
            if not conversation_id:
                continue
            cursor = self._thread_cursors.get(root_id)
            try:
                page = await self.api.get_thread(root_id, after=cursor, limit=MAX_POLL_PAGE)
            except OrbitAPIError as exc:
                if exc.status in (403, 404):
                    self._thread_roots.pop(root_id, None)
                    self._thread_cursors.pop(root_id, None)
                    self._thread_signature.pop(root_id, None)
                continue
            conversation = self._conversations.get(conversation_id) or {}
            replies = sorted(page.get("items") or [], key=_message_sort_key)
            for reply in replies:
                if reply.get("thread_root_id") is None:
                    reply["thread_root_id"] = root_id
                await self._dispatch(reply, conversation)
            if replies:
                self._thread_cursors[root_id] = _message_sort_key(replies[-1])

    # ── inbound dispatch ────────────────────────────────────────────────────

    async def _dispatch(self, message: dict, conversation: dict) -> None:
        """Normalise one Orbit message and hand it to the gateway (idempotent)."""
        message_id = str(message.get("id") or "")
        if not message_id or message.get("deleted"):
            return
        if str(message.get("kind") or "message") != "message":
            return
        author_id = str(message.get("author_id") or "")
        if not author_id or author_id == self._self_id:
            return
        if self._dedup.is_duplicate(message_id):
            return

        conversation_id = str(message.get("conversation_id") or conversation.get("id") or "")
        thread_root = str(message.get("thread_root_id") or "") or None
        is_dm = self._conversation_type(conversation) == "dm"
        body = str(message.get("body") or "")
        text = body.strip()

        attachments = message.get("attachments") or []
        if not text and attachments:
            names = ", ".join(str(a.get("file_name") or "file") for a in attachments)
            text = f"[attachment: {names}]"
        if not text:
            return

        addressed, text = self._addressing(text, message, thread_root, is_dm)
        if not addressed:
            logger.debug("Orbit: ignoring message %s in %s (not addressed)", message_id, conversation_id)
            return

        # Auto-thread (Discord ``auto_thread`` parity): a channel mention gets its own thread so
        # the answer never lands in the shared channel.  Orbit threads are replies rooted at a
        # message (``thread_root_id`` is the root message id) and have NO thread-create or
        # rename API: the inbound message simply *becomes* the root, the bot replies with
        # ``thread_root_id=message_id``, and the thread's only name is the root message text
        # itself.  Discord's semantic pre-seeded thread name with a later semantic rename has no
        # Orbit equivalent, so no naming is attempted — by design (see README "Limitations").
        # DMs stay inline, messages already in a thread keep their root, and the
        # ``require_mention`` gate above ran first, same order as Discord.
        auto_thread_created = False
        auto_thread_initial_name: str | None = None
        if (
            self.auto_thread
            and not is_dm
            and thread_root is None
            and conversation_id not in self.no_thread_channels
        ):
            thread_root = message_id
            auto_thread_created = True
            auto_thread_initial_name = self._auto_thread_name(text)

        reply_to = message.get("reply_to") or {}
        source = self.build_source(
            chat_id=conversation_id,
            chat_name=self._conversation_name(conversation),
            chat_type="dm" if is_dm else ("thread" if thread_root else "group"),
            user_id=author_id,
            user_name=self._member_names.get(author_id) or author_id,
            thread_id=thread_root,
            guild_id=self.workspace_id,
            message_id=message_id,
            auto_thread_created=auto_thread_created,
            auto_thread_initial_name=auto_thread_initial_name,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            user_id=author_id,
            user_name=self._member_names.get(author_id) or author_id,
            source=source,
            message_id=message_id,
            reply_to_message_id=str(message.get("reply_to_id") or "") or None,
            reply_to_text=str(reply_to.get("body") or "") or None,
            reply_to_author_id=str(reply_to.get("author_id") or "") or None,
            reply_to_is_own_message=str(reply_to.get("author_id") or "") == self._self_id,
            timestamp=_parse_timestamp(message.get("created_at")),
            raw_message=message,
        )
        logger.info(
            "Orbit: inbound %s in %s%s from %s",
            message_id, conversation_id,
            f" thread {thread_root}" if thread_root else "", author_id,
        )
        await self.handle_message(event)

    def _addressing(
        self, text: str, message: dict, thread_root: str | None, is_dm: bool
    ) -> tuple[bool, str]:
        """Decide whether a message addresses the bot, and strip a leading mention."""
        if is_dm:
            return True, text
        mentions = (message.get("mentions") or {}).get("user_ids") or []
        if self._self_id and self._self_id in [str(uid) for uid in mentions]:
            return True, self._strip_mention_tokens(text)
        if self._self_id and f"<@{self._self_id}>" in text:
            return True, self._strip_mention_tokens(text)
        reply_to = message.get("reply_to") or {}
        if self._self_id and str(reply_to.get("author_id") or "") == self._self_id:
            return True, text
        if thread_root and thread_root in self._bot_threads:
            # A thread the bot already took part in is a conversation, not a broadcast.
            return True, text
        for alias in self._mention_aliases():
            stripped = self._strip_alias(text, alias)
            if stripped is not None:
                return True, stripped
        if not self.require_mention:
            return True, text
        return False, text

    def _mention_aliases(self) -> list[str]:
        """Names that address the bot in channel text: config alias, display name, its first word."""
        candidates = [self.mention_name]
        display = (self._self_name or "").strip()
        if display:
            candidates.extend([display, display.split()[0]])
        candidates.append("hermes")
        seen: list[str] = []
        for candidate in candidates:
            alias = (candidate or "").strip().lower()
            if alias and alias not in seen:
                seen.append(alias)
        return seen

    @staticmethod
    def _strip_mention_tokens(text: str) -> str:
        return _MENTION_TOKEN_RE.sub("", text).strip() or text

    @staticmethod
    def _auto_thread_name(text: str) -> str:
        """The thread's "name": the mention-stripped root text, first 80 characters.

        Orbit threads have no name field and no rename route — a thread is identified by its
        root message, and that message's text is the only name a member sees.  Discord renames
        its auto-thread to the agent session title afterwards; that lane is Discord-only and
        deliberately not implemented here (there is nothing to call).
        """
        cleaned = re.sub(r"\s+", " ", _MENTION_TOKEN_RE.sub("", text or "")).strip()
        return cleaned[:80] or "Orbit thread"

    @staticmethod
    def _strip_alias(text: str, alias: str) -> str | None:
        """Strip ``@alias`` / ``alias:`` from the start; None when it is not there."""
        if not alias:
            return None
        pattern = re.compile(
            rf"^\s*(?:@|\b){re.escape(alias)}\b\s*[:,]?\s*", re.IGNORECASE
        )
        match = pattern.match(text)
        if not match:
            return None
        remainder = text[match.end():].strip()
        return remainder or text

    # ── live websocket (best effort) ─────────────────────────────────────────

    async def _live_loop(self) -> None:
        """Subscribe to ``/live`` for low latency; polling remains authoritative."""
        try:
            import websockets  # noqa: F401 - probe only; the API opens the socket
        except ImportError:
            logger.info("Orbit: websockets not installed; using polling only")
            return
        delay = self._reconnect_delay
        while True:
            try:
                self._ws = await self.api.open_live(epoch=self._live_epoch, after=self._live_seq)
                delay = self._reconnect_delay
                logger.info("Orbit: live socket connected (after=%s)", self._live_seq)
                async for raw in self._ws:
                    await self._handle_live_frame(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on any socket error
                logger.info("Orbit: live socket closed (%s); polling continues", exc)
            finally:
                await self._close_ws()
            await asyncio.sleep(min(30.0, delay) + random.uniform(0, 0.5))
            delay = min(delay * 2, 30.0)

    async def _handle_live_frame(self, raw: Any) -> None:
        try:
            frame = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(frame, dict):
            return
        kind = frame.get("type")
        if kind == "hello":
            self._live_epoch = frame.get("epoch") or self._live_epoch
            seq = frame.get("seq")
            self._live_seq = int(seq) if isinstance(seq, (int, float)) else self._live_seq
            return
        if kind:  # resync / presence and other control frames
            return
        seq = frame.get("seq")
        if isinstance(seq, (int, float)):
            self._live_seq = int(seq)
        if frame.get("topic") != "chat":
            return
        event = frame.get("event") or {}
        if event.get("type") not in ("message.created", "message.updated"):
            return
        message = event.get("message") or {}
        conversation_id = str(message.get("conversation_id") or "")
        conversation = self._conversations.get(conversation_id)
        if conversation is None:
            with contextlib.suppress(OrbitAPIError):
                await self._refresh_conversations()
            conversation = self._conversations.get(conversation_id) or {}
        if message.get("thread_root_id") is None:
            await self._prime_thread(message)
        await self._dispatch(message, conversation)


def _orbit_platform() -> Platform:
    """Resolve the dynamic ``orbit`` platform member (registered by ``register``)."""
    try:
        return Platform(PLATFORM_NAME)
    except ValueError:
        return Platform._add_pseudo_member(PLATFORM_NAME)  # type: ignore[attr-defined]


# ── plugin hooks ────────────────────────────────────────────────────────────


def _cfg(config) -> tuple[dict, str, str]:
    extra = getattr(config, "extra", {}) or {}
    base_url = str(extra_or_secret(extra, "base_url", "ORBIT_BASE_URL", "")).strip()
    workspace_id = str(extra_or_secret(extra, "workspace_id", "ORBIT_WORKSPACE_ID", "")).strip()
    return extra, base_url, workspace_id


def check_requirements() -> bool:
    """Passive probe: configured enough to attempt a connection (httpx is always present)."""
    base_url = str(get_scoped_secret("ORBIT_BASE_URL", "") or "").strip()
    workspace_id = str(get_scoped_secret("ORBIT_WORKSPACE_ID", "") or "").strip()
    return bool(base_url and workspace_id)


def validate_config(config) -> bool:
    """Enough to connect AND a way to authenticate."""
    extra, base_url, workspace_id = _cfg(config)
    if not (base_url and workspace_id):
        return False
    cookie = str(extra_or_secret(extra, "session_cookie", "ORBIT_SESSION_COOKIE", "")).strip()
    email = str(extra_or_secret(extra, "email", "ORBIT_EMAIL", "")).strip()
    password = str(extra_or_secret(extra, "password", "ORBIT_PASSWORD", ""))
    return bool(cookie or (email and password))


def is_connected(config) -> bool:
    """``hermes gateway status`` / setup UI: fully configured (env or config.yaml)."""
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env before the adapter is constructed."""
    base_url = str(get_scoped_secret("ORBIT_BASE_URL", "") or "").strip()
    workspace_id = str(get_scoped_secret("ORBIT_WORKSPACE_ID", "") or "").strip()
    if not (base_url and workspace_id):
        return None
    seed = seed_extra_from_env(
        (
            ("ORBIT_EMAIL", "email", None),
            ("ORBIT_PASSWORD", "password", None),
            ("ORBIT_SESSION_COOKIE", "session_cookie", None),
            ("ORBIT_SESSION_COOKIE_NAME", "session_cookie_name", None),
            ("ORBIT_MENTION_NAME", "mention_name", None),
            ("ORBIT_POLL_INTERVAL", "poll_interval", float),
            ("ORBIT_USE_WEBSOCKET", "use_websocket", lambda v: v.strip().lower() in _TRUTHY),
            ("ORBIT_REQUIRE_MENTION", "require_mention", lambda v: v.strip().lower() in _TRUTHY),
            ("ORBIT_AUTO_THREAD", "auto_thread", lambda v: v.strip().lower() in _TRUTHY),
            ("ORBIT_NO_THREAD_CHANNELS", "no_thread_channels", None),
        ),
        home_env="ORBIT_HOME_CHANNEL",
    )
    return {"base_url": base_url, "workspace_id": workspace_id, **seed}


_YAML_SPEC = (
    ("base_url", "ORBIT_BASE_URL", "str"),
    ("workspace_id", "ORBIT_WORKSPACE_ID", "str"),
    ("email", "ORBIT_EMAIL", "str"),
    ("password", "ORBIT_PASSWORD", "str"),
    ("session_cookie", "ORBIT_SESSION_COOKIE", "str"),
    ("session_cookie_name", "ORBIT_SESSION_COOKIE_NAME", "str"),
    ("home_channel", "ORBIT_HOME_CHANNEL", "str"),
    ("mention_name", "ORBIT_MENTION_NAME", "str"),
    ("poll_interval", "ORBIT_POLL_INTERVAL", "str"),
    ("use_websocket", "ORBIT_USE_WEBSOCKET", "lower"),
    ("require_mention", "ORBIT_REQUIRE_MENTION", "lower"),
    ("auto_thread", "ORBIT_AUTO_THREAD", "lower"),
    ("no_thread_channels", "ORBIT_NO_THREAD_CHANNELS", "csv"),
    ("allowed_users", "ORBIT_ALLOWED_USERS", "csv"),
    ("allow_all_users", "ORBIT_ALLOW_ALL_USERS", "lower"),
)


def apply_yaml_config(yaml_cfg, platform_cfg) -> dict | None:
    """``apply_yaml_config_fn``: bridge ``gateway.platforms.orbit.*`` keys to env + extra."""
    cfg = dict(yaml_cfg or {})
    if not cfg and isinstance(platform_cfg, dict):
        cfg = dict(platform_cfg.get("extra") or {})
    return apply_yaml_bridge(cfg, _YAML_SPEC)


async def _standalone_send(
    pconfig, chat_id: str, message: str, *, thread_id: str | None = None,
    media_files: list[str] | None = None, force_document: bool = False,
) -> dict[str, Any]:
    """Out-of-process delivery (cron without a co-resident gateway): log in and send."""
    extra, base_url, workspace_id = _cfg(pconfig)
    if not (base_url and workspace_id):
        return send_error("Orbit standalone send: ORBIT_BASE_URL and ORBIT_WORKSPACE_ID must be set")
    api = OrbitAPI(
        base_url,
        workspace_id,
        email=str(extra_or_secret(extra, "email", "ORBIT_EMAIL", "")).strip(),
        password=str(extra_or_secret(extra, "password", "ORBIT_PASSWORD", "")),
        session_cookie=str(extra_or_secret(extra, "session_cookie", "ORBIT_SESSION_COOKIE", "")),
        cookie_name=str(extra_or_secret(extra, "session_cookie_name", "ORBIT_SESSION_COOKIE_NAME", "")),
    )
    try:
        await api.login()
        chunks = BasePlatformAdapter.truncate_message(
            message, max_length=MAX_BODY_CHARS
        ) if message else []
        sent_ids: list[str] = []
        for index, chunk in enumerate(chunks):
            if index == 0 and media_files and not force_document:
                uploaded = await api.upload_file(media_files[0])
                message_record = await api.send_message(
                    str(chat_id), chunk, thread_root_id=thread_id,
                    file_ids=[str(uploaded.get("id") or "")],
                )
            else:
                message_record = await api.send_message(
                    str(chat_id), chunk, thread_root_id=thread_id
                )
            sent_ids.append(str(message_record.get("id") or ""))
        if not sent_ids:
            return send_error("Orbit standalone send: empty message")
        return {"success": True, "message_id": sent_ids[-1]}
    except OrbitAPIError as exc:
        logger.debug("Orbit standalone send failed", exc_info=True)
        return send_error(f"Orbit standalone send failed: {exc}")
    finally:
        await api.close()


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Orbit",
        dashboard_description="Reply in Orbit channels, threads and DMs as a workspace member.",
        docs_url="https://github.com/simpros/hermes-orbit-gateway#readme",
        adapter_factory=OrbitAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ORBIT_BASE_URL", "ORBIT_WORKSPACE_ID"],
        install_hint="No extra packages needed (httpx ships with Hermes)",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=apply_yaml_config,
        cron_deliver_env_var="ORBIT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ORBIT_ALLOWED_USERS",
        allow_all_env="ORBIT_ALLOW_ALL_USERS",
        max_message_length=MAX_BODY_CHARS,
        emoji="🛰️",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are chatting in Orbit, a team chat. Messages are Markdown (bold, lists, "
            "fenced code blocks) and the body limit is 4000 characters — longer replies are "
            "split automatically. In channels, answer where you were addressed; in threads, "
            "reply into the thread. Mentions are written as <@user_id> in message bodies."
        ),
    )
