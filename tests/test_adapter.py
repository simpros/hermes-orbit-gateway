"""Unit tests: session auth, Orbit → Hermes mapping, chunking, plugin wiring."""

from __future__ import annotations

import json

import httpx
import pytest

BASE = "https://orbit.example.test"
WS = "ws-1"


# ── helpers ─────────────────────────────────────────────────────────────────


class Recorder:
    """A fake Orbit server built on ``httpx.MockTransport``.

    ``routes`` maps ``(method, path-prefix)`` to either a response factory or a
    list of responses consumed in order (the last one repeats).
    """

    def __init__(self, orbit, routes=None):
        self.orbit = orbit
        self.requests: list[httpx.Request] = []
        self.phases: list[dict] = []
        # Every fake server signs in unless a test overrides the login route.
        self._routes = {
            ("POST", "/api/v1/auth/login"): lambda request: httpx.Response(
                200,
                json={"user": {"id": "bot-user"}, "session_id": "s0"},
                headers={"set-cookie": "orbit_session_dev=test-session; Path=/; HttpOnly"},
            ),
            **(routes or {}),
        }
        self.api = orbit.OrbitAPI(
            BASE, WS, email="bot@example.test", password="secret"
        )
        self.api._client = httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        body = None
        if request.content:
            try:
                body = json.loads(request.content)
            except ValueError:
                body = None
        self.phases.append({"method": request.method, "path": path, "body": body,
                            "headers": dict(request.headers)})
        response = self._route(request.method, path)
        if callable(response):
            return response(request)
        if isinstance(response, list):
            if len(response) > 1:
                return response.pop(0)
            return response[0]
        return response

    def _route(self, method: str, path: str) -> object:
        for (want_method, want_path), response in self._routes.items():
            if method == want_method and path.startswith(want_path):
                return response
        return httpx.Response(404, json={"code": "chat_not_found", "detail": "no route"})

    def sent_messages(self) -> list[dict]:
        return [p["body"] for p in self.phases if p["body"] and "nonce" in p["body"]]


def message(
    message_id: str = "0190-m1",
    *,
    body: str = "hello",
    author: str = "human-1",
    conversation: str = "conv-1",
    thread_root: str | None = None,
    mentions: list[str] | None = None,
    reply_to_author: str | None = None,
    created_at: str = "2026-11-04T10:00:00.000Z",
) -> dict:
    payload = {
        "id": message_id,
        "conversation_id": conversation,
        "thread_root_id": thread_root,
        "kind": "message",
        "author_id": author,
        "body": body,
        "mentions": {"user_ids": mentions or [], "channel": False, "here": False},
        "created_at": created_at,
        "deleted": False,
        "attachments": [],
        "reply_count": 0,
        "last_reply_at": None,
        "reply_to_id": None,
        "reply_to": (
            {"id": "0190-parent", "author_id": reply_to_author, "body": "earlier", "sticker": False}
            if reply_to_author
            else None
        ),
    }
    return payload


def conversation(conversation_id: str = "conv-1", *, kind: str = "public", name: str = "general"):
    return {
        "id": conversation_id,
        "kind": kind,
        "name": name,
        "topic": "",
        "member_ids": ["bot-user", "human-1"],
        "is_default": False,
        "self_dm": False,
        "archived": False,
    }


def capture(adapter) -> list:
    """Record what the adapter would hand to the gateway."""
    events: list = []

    async def handler(event):
        events.append(event)

    adapter.handle_message = handler
    return events


# ── session auth ────────────────────────────────────────────────────────────


async def test_login_keeps_session_cookie_and_sends_origin(orbit):
    rec = Recorder(
        orbit,
        {
            ("POST", "/api/v1/auth/login"): lambda request: httpx.Response(
                200,
                json={"user": {"id": "bot-user"}, "session_id": "s1"},
                headers={"set-cookie": "orbit_session_dev=token-123; Path=/; HttpOnly"},
            ),
            ("GET", "/api/v1/auth/me"): httpx.Response(200, json={"id": "bot-user"}),
        },
    )
    await rec.api.login()
    assert rec.api.session_headers() == {"Cookie": "orbit_session_dev=token-123"}

    await rec.api.me()
    me_call = [p for p in rec.phases if p["path"].endswith("/auth/me")][0]
    assert me_call["headers"]["origin"] == BASE
    assert me_call["headers"]["cookie"] == "orbit_session_dev=token-123"


async def test_login_rejects_two_factor_accounts(orbit):
    rec = Recorder(
        orbit,
        {
            ("POST", "/api/v1/auth/login"): httpx.Response(
                202, json={"code": "two_factor_required"}
            )
        },
    )
    with pytest.raises(orbit.OrbitAuthError):
        await rec.api.login()


async def test_bad_password_is_an_auth_error(orbit):
    rec = Recorder(
        orbit,
        {
            ("POST", "/api/v1/auth/login"): httpx.Response(
                401, json={"code": "invalid_credentials", "detail": "Invalid email or password."}
            )
        },
    )
    with pytest.raises(orbit.OrbitAuthError) as excinfo:
        await rec.api.login()
    assert "Invalid email or password" in str(excinfo.value)


async def test_session_cookie_from_env_is_used_without_login(orbit):
    api = orbit.OrbitAPI(BASE, WS, session_cookie="orbit_session_dev=seed-cookie")
    rec = Recorder(orbit, {("GET", "/api/v1/auth/me"): httpx.Response(200, json={"id": "bot-user"})})
    rec.api = api
    api._client = httpx.AsyncClient(base_url=BASE, transport=httpx.MockTransport(rec._handle))
    assert (await api.me())["id"] == "bot-user"
    assert not [p for p in rec.phases if p["path"].endswith("/auth/login")]


async def test_bare_session_token_uses_bearer_auth(orbit):
    api = orbit.OrbitAPI(BASE, WS, session_cookie="opaque-token-value")
    assert api.session_headers() == {"Authorization": "Bearer opaque-token-value"}


async def test_401_triggers_one_relogin_and_retry(orbit):
    state = {"logins": 0}

    def login(request):
        state["logins"] += 1
        return httpx.Response(
            200, json={"user": {"id": "bot-user"}, "session_id": "s2"},
            headers={"set-cookie": "__Host-orbit_session=fresh; Path=/"},
        )

    def messages(request):
        if request.headers.get("cookie") == "__Host-orbit_session=fresh":
            return httpx.Response(200, json={"items": []})
        return httpx.Response(401, json={"code": "authentication_required"})

    rec = Recorder(
        orbit,
        {
            ("POST", "/api/v1/auth/login"): login,
            ("GET", "/api/v1/workspaces/ws-1/chat/conversations/conv-1/messages"): messages,
        },
    )
    # Seed an expired cookie so the first request is rejected.
    rec.api._cookie = "orbit_session_dev=expired"
    rec.api._has_session = True

    assert await rec.api.list_messages("conv-1", after="x") == []
    assert state["logins"] == 1
    assert rec.api._cookie == "__Host-orbit_session=fresh"


async def test_list_conversations_returns_the_json_array(orbit):
    rec = Recorder(
        orbit,
        {("GET", "/api/v1/workspaces/ws-1/chat/conversations"):
            httpx.Response(200, json=[{"id": "conv-1", "kind": "public"}])},
    )
    await rec.api.login()
    assert [c["id"] for c in await rec.api.list_conversations()] == ["conv-1"]


# ── API request shapes ──────────────────────────────────────────────────────


async def test_send_message_payload_has_nonce_and_thread_root(orbit):
    rec = Recorder(
        orbit,
        {("POST", "/api/v1/workspaces/ws-1/chat/conversations/conv-1/messages"):
            httpx.Response(200, json={"result": {"id": "0190-new"}, "events": [], "seq": 1})},
    )
    await rec.api.login()
    await rec.api.send_message("conv-1", "hello", thread_root_id="root-1", reply_to_id="0190-q")

    body = rec.sent_messages()[0]
    assert body["body"] == "hello"
    assert body["thread_root_id"] == "root-1"
    assert body["reply_to_id"] == "0190-q"
    assert body["file_ids"] == []
    assert len(body["nonce"]) >= 8
    post = [p for p in rec.phases if p["method"] == "POST"][-1]
    assert post["headers"]["origin"] == BASE


async def test_poll_uses_after_cursor_and_limit(orbit):
    rec = Recorder(
        orbit,
        {("GET", "/api/v1/workspaces/ws-1/chat/conversations/conv-1/messages"):
            httpx.Response(200, json={"items": []})},
    )
    await rec.api.login()
    await rec.api.list_messages("conv-1", after="0190-cursor")
    request = [r for r in rec.requests if r.url.path.endswith("/messages")][0]
    assert request.url.params["after"] == "0190-cursor"
    assert int(request.url.params["limit"]) <= 100


async def test_open_dm_posts_user_ids(orbit):
    rec = Recorder(
        orbit,
        {("POST", "/api/v1/workspaces/ws-1/chat/dms"):
            httpx.Response(200, json={"result": {"id": "dm-9"}, "events": [], "seq": 2})},
    )
    await rec.api.login()
    conversation = await rec.api.open_dm(["human-1"])
    assert conversation["id"] == "dm-9"
    post = [p for p in rec.phases if p["path"].endswith("/chat/dms")][0]
    assert post["body"]["user_ids"] == ["human-1"]


async def test_upload_file_posts_multipart(orbit, tmp_path):
    sample = tmp_path / "chart.png"
    sample.write_bytes(b"\x89PNG\r\n\x1a\nfake")
    rec = Recorder(
        orbit,
        {("POST", "/api/v1/workspaces/ws-1/chat/files"):
            httpx.Response(201, json={"id": "file-7", "file_name": "chart.png"})},
    )
    await rec.api.login()
    uploaded = await rec.api.upload_file(str(sample))
    assert uploaded["id"] == "file-7"
    request = rec.requests[-1]
    assert request.headers["content-type"].startswith("multipart/form-data")


# ── message mapping ─────────────────────────────────────────────────────────


async def test_channel_mention_opens_a_thread_rooted_at_the_inbound_message(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="<@bot-user> what is the deploy status?"), conversation())

    assert len(events) == 1
    event = events[0]
    assert event.source.chat_id == "conv-1"
    assert event.source.chat_type == "thread"
    assert event.source.thread_id == "0190-m1"
    assert event.source.auto_thread_created is True
    assert event.source.auto_thread_initial_name == "what is the deploy status?"
    assert event.source.user_id == "human-1"
    assert event.source.user_name == "Ada"
    assert event.text == "what is the deploy status?"


async def test_no_thread_channel_answers_inline(adapter):
    adapter.no_thread_channels = {"conv-1"}
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="<@bot-user> keep it here"), conversation())

    assert len(events) == 1
    event = events[0]
    assert event.source.chat_type == "group"
    assert event.source.thread_id is None
    assert event.source.auto_thread_created is False


async def test_auto_thread_can_be_disabled(adapter):
    adapter.auto_thread = False
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="<@bot-user> inline please"), conversation())

    assert len(events) == 1
    event = events[0]
    assert event.source.chat_type == "group"
    assert event.source.thread_id is None
    assert event.source.auto_thread_created is False


async def test_auto_thread_name_caps_the_root_text_at_80_chars(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="<@bot-user> " + "x" * 120), conversation())

    assert len(events[0].source.auto_thread_initial_name) == 80


async def test_channel_message_without_mention_is_ignored(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="lunch at noon"), conversation())
    assert events == []


async def test_at_name_alias_addresses_the_bot(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="@hermes: summarise #12"), conversation())
    assert [e.text for e in events] == ["summarise #12"]


async def test_reply_to_bot_counts_as_addressed(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="and then?", reply_to_author="bot-user"), conversation())
    assert len(events) == 1
    assert events[0].reply_to_is_own_message is True


async def test_dm_needs_no_mention(adapter):
    events = capture(adapter)
    dm = conversation("dm-1", kind="dm", name="")
    adapter._conversations = {"dm-1": dm}
    await adapter._dispatch(message(body="ping", conversation="dm-1"), dm)

    assert len(events) == 1
    assert events[0].source.chat_type == "dm"
    assert events[0].source.thread_id is None
    assert events[0].text == "ping"


async def test_thread_reply_keeps_thread_context(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    adapter._bot_threads.add("root-1")
    reply = message(body="follow-up detail", thread_root="root-1", message_id="0190-m2")
    await adapter._dispatch(reply, conversation())

    assert len(events) == 1
    event = events[0]
    assert event.source.chat_type == "thread"
    assert event.source.thread_id == "root-1"
    assert event.source.chat_id == "conv-1"


async def test_thread_followed_by_bot_needs_no_mention(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    adapter._bot_threads.add("root-1")
    await adapter._dispatch(message(body="plain thread chat", thread_root="root-1"), conversation())
    assert len(events) == 1


async def test_unknown_thread_requires_mention(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="plain thread chat", thread_root="root-9"), conversation())
    assert events == []


async def test_own_messages_are_skipped(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    await adapter._dispatch(message(body="<@bot-user> hi", author="bot-user"), conversation())
    assert events == []


async def test_duplicate_delivery_is_deduplicated(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    payload = message(body="<@bot-user> once")
    await adapter._dispatch(payload, conversation())
    await adapter._dispatch(payload, conversation())
    assert len(events) == 1


async def test_deleted_and_system_messages_are_skipped(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    deleted = message(body="<@bot-user> gone")
    deleted["deleted"] = True
    system = message(message_id="0190-sys", body="<@bot-user> joined")
    system["kind"] = "system"
    await adapter._dispatch(deleted, conversation())
    await adapter._dispatch(system, conversation())
    assert events == []


async def test_attachment_only_message_keeps_a_placeholder(adapter):
    events = capture(adapter)
    adapter._conversations = {"conv-1": conversation()}
    payload = message(body="", conversation="dm-1")
    payload["attachments"] = [{"id": "f1", "file_name": "spec.pdf"}]
    dm = conversation("dm-1", kind="dm", name="")
    await adapter._dispatch(payload, dm)
    assert events[0].text == "[attachment: spec.pdf]"


async def test_get_chat_info_names_channels_and_dms(adapter):
    adapter._conversations = {
        "conv-1": conversation(),
        "dm-1": conversation("dm-1", kind="dm", name=""),
    }
    assert await adapter.get_chat_info("conv-1") == {
        "name": "general", "type": "group", "chat_id": "conv-1",
    }
    info = await adapter.get_chat_info("dm-1")
    assert info["type"] == "dm"
    assert info["name"] == "DM with Ada"


# ── outbound ────────────────────────────────────────────────────────────────


async def test_send_posts_body_nonce_and_reply_anchor(adapter, fake_api):
    adapter.api = fake_api({"result": {"id": "0190-answer"}})
    result = await adapter.send("conv-1", "the answer", reply_to="0190-question")
    assert result.success and result.message_id == "0190-answer"

    sent = adapter.api.sent_messages()[0]
    assert sent["body"] == "the answer"
    assert sent["reply_to_id"] == "0190-question"
    assert sent["thread_root_id"] is None


async def test_send_into_thread_uses_thread_root(adapter, fake_api):
    adapter.api = fake_api({"result": {"id": "0190-reply"}})
    await adapter.send("conv-1", "in thread", metadata={"thread_id": "root-1"})
    sent = adapter.api.sent_messages()[0]
    assert sent["thread_root_id"] == "root-1"
    assert sent.get("reply_to_id") is None


async def test_long_messages_are_chunked_under_the_body_cap(adapter, fake_api):
    adapter.api = fake_api({"result": {"id": "0190-chunk"}})
    body = "\n\n".join(f"paragraph {i} " + "x" * 400 for i in range(20))
    result = await adapter.send("conv-1", body)

    sent = adapter.api.sent_messages()
    assert result.success
    assert len(sent) > 1
    assert all(len(chunk["body"]) <= adapter.MAX_MESSAGE_LENGTH for chunk in sent)
    assert "(1/" in sent[0]["body"]
    assert result.continuation_message_ids == tuple("0190-chunk" for _ in sent[:-1])


async def test_empty_message_is_refused(adapter):
    result = await adapter.send("conv-1", "   ")
    assert not result.success
    assert "empty" in (result.error or "").lower()


async def test_send_failure_is_reported_not_raised(adapter, fake_api):
    adapter.api = fake_api(error_status=403, error_detail="no access")
    result = await adapter.send("conv-1", "hello")
    assert not result.success
    assert "no access" in (result.error or "")


# ── plugin hooks ────────────────────────────────────────────────────────────


def test_validate_config_requires_auth(orbit):
    from gateway.config import PlatformConfig

    ok = PlatformConfig(
        enabled=True,
        extra={"base_url": BASE, "workspace_id": WS, "email": "a@b.c", "password": "x"},
    )
    cookie_only = PlatformConfig(
        enabled=True,
        extra={"base_url": BASE, "workspace_id": WS, "session_cookie": "orbit_session_dev=t"},
    )
    missing_auth = PlatformConfig(
        enabled=True, extra={"base_url": BASE, "workspace_id": WS, "email": "a@b.c"}
    )
    assert orbit.validate_config(ok)
    assert orbit.validate_config(cookie_only)
    assert not orbit.validate_config(missing_auth)


def test_env_enablement_seeds_extra(orbit, monkeypatch):
    monkeypatch.setenv("ORBIT_BASE_URL", BASE)
    monkeypatch.setenv("ORBIT_WORKSPACE_ID", WS)
    monkeypatch.setenv("ORBIT_EMAIL", "bot@example.test")
    monkeypatch.setenv("ORBIT_HOME_CHANNEL", "conv-9")
    monkeypatch.setenv("ORBIT_AUTO_THREAD", "false")
    monkeypatch.setenv("ORBIT_NO_THREAD_CHANNELS", "conv-7, conv-8")
    seeded = orbit._env_enablement()
    assert seeded["base_url"] == BASE
    assert seeded["workspace_id"] == WS
    assert seeded["email"] == "bot@example.test"
    assert seeded["auto_thread"] is False
    assert seeded["no_thread_channels"] == "conv-7, conv-8"
    assert seeded["home_channel"] == {"chat_id": "conv-9", "name": "Home"}


def test_env_enablement_is_none_without_required_vars(orbit, monkeypatch):
    monkeypatch.delenv("ORBIT_BASE_URL", raising=False)
    monkeypatch.delenv("ORBIT_WORKSPACE_ID", raising=False)
    assert orbit._env_enablement() is None


def test_yaml_bridge_maps_platform_keys(orbit, monkeypatch):
    monkeypatch.delenv("ORBIT_BASE_URL", raising=False)
    monkeypatch.delenv("ORBIT_AUTO_THREAD", raising=False)
    monkeypatch.delenv("ORBIT_NO_THREAD_CHANNELS", raising=False)
    seeded = orbit.apply_yaml_config(
        {
            "base_url": BASE,
            "workspace_id": WS,
            "use_websocket": False,
            "auto_thread": False,
            "no_thread_channels": ["conv-7", "conv-8"],
        },
        {},
    )
    assert seeded["base_url"] == BASE
    assert seeded["use_websocket"] is False
    assert seeded["auto_thread"] is False
    assert seeded["no_thread_channels"] == ["conv-7", "conv-8"]


def test_register_wires_the_platform(orbit):
    class Ctx:
        def __init__(self):
            self.kwargs = None

        def register_platform(self, **kwargs):
            self.kwargs = kwargs

    ctx = Ctx()
    orbit.register(ctx)
    kwargs = ctx.kwargs
    assert kwargs["name"] == "orbit"
    assert kwargs["adapter_factory"] is orbit.OrbitAdapter
    assert kwargs["cron_deliver_env_var"] == "ORBIT_HOME_CHANNEL"
    assert kwargs["env_enablement_fn"] is orbit._env_enablement
    assert kwargs["apply_yaml_config_fn"] is orbit.apply_yaml_config
    assert callable(kwargs["standalone_sender_fn"])
    assert kwargs["max_message_length"] == orbit.MAX_BODY_CHARS


def test_adapter_is_a_hermes_platform_adapter(orbit):
    from gateway.platforms.base import BasePlatformAdapter

    assert issubclass(orbit.OrbitAdapter, BasePlatformAdapter)
    assert orbit.OrbitAdapter.splits_long_messages is True
