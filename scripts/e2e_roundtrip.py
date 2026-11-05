#!/usr/bin/env python3
"""End-to-end round-trip against a real Orbit instance.

Proves the whole path the gateway depends on:

    Orbit message  ->  session login + poll  ->  adapter mapping  ->  handler
                                                                     |
    Orbit message  <-  POST /messages  <-  adapter.send  <------------+

Three cases are exercised: a channel mention, a thread reply, and a DM.  The
agent turn is stubbed (the handler echoes), so this validates transport and
mapping only — which is exactly what a gateway bug would break.

Environment:

    ORBIT_BASE_URL, ORBIT_WORKSPACE_ID
    ORBIT_EMAIL / ORBIT_PASSWORD              bot account
    ORBIT_PEER_EMAIL / ORBIT_PEER_PASSWORD    a second member (the "human")
    ORBIT_TEST_CHANNEL                        optional conversation id
    HERMES_AGENT_ROOT                         optional Hermes checkout path

Exit code 0 only when every case passed.
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_adapter():
    spec = importlib.util.spec_from_file_location("orbit_adapter_e2e", REPO_ROOT / "adapter.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load adapter.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["orbit_adapter_e2e"] = module
    spec.loader.exec_module(module)
    return module


def _hermes_path() -> None:
    root = os.environ.get("HERMES_AGENT_ROOT") or os.path.expanduser("~/.hermes/hermes-agent")
    if Path(root).is_dir() and root not in sys.path:
        sys.path.insert(0, root)


_hermes_path()
if os.environ.get("ORBIT_E2E_LOG"):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
orbit = load_adapter()

from gateway.config import PlatformConfig  # noqa: E402
from gateway.platforms.event import MessageEvent  # noqa: E402

BASE_URL = os.environ["ORBIT_BASE_URL"]
WORKSPACE_ID = os.environ["ORBIT_WORKSPACE_ID"]
BOT_EMAIL = os.environ.get("ORBIT_EMAIL", "")
BOT_PASSWORD = os.environ.get("ORBIT_PASSWORD", "")
PEER_EMAIL = os.environ.get("ORBIT_PEER_EMAIL", "")
PEER_PASSWORD = os.environ.get("ORBIT_PEER_PASSWORD", "")

received: list[MessageEvent] = []
replies: list[dict] = []
waiter = asyncio.Event()
results: list[tuple[str, bool, str]] = []


def log(line: str) -> None:
    print(f"[e2e] {line}", flush=True)


async def handler(event: MessageEvent) -> None:
    """Stub of the gateway turn: record the inbound event and answer it."""
    received.append(event)
    thread = event.source.thread_id
    log(
        f"inbound chat={event.source.chat_id} type={event.source.chat_type} "
        f"thread={thread} from={event.source.user_id} msg={event.message_id} text={event.text!r}"
    )
    metadata = {"thread_id": thread} if thread else None
    # Recorded before the await so the collector never misses a fast reply.
    record = {"inbound": event, "success": False, "message_id": None, "error": "pending", "done": False}
    replies.append(record)
    answer = await adapter.send(
        event.source.chat_id, f"hermes received: {event.text}", metadata=metadata
    )
    record.update(
        success=answer.success, message_id=answer.message_id, error=answer.error, done=True
    )
    log(f"reply chat={event.source.chat_id} thread={thread} msg={answer.message_id}")
    waiter.set()


async def wait_for(predicate, timeout: float = 45.0, what: str = "event"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for event in list(received):
            if predicate(event):
                return event
        waiter.clear()
        try:
            await asyncio.wait_for(waiter.wait(), timeout=min(2.0, deadline - time.monotonic()))
        except TimeoutError:
            continue
    raise TimeoutError(f"timed out waiting for {what}")


async def reply_for(event: MessageEvent, timeout: float = 30.0) -> dict:
    """Wait for the handler's own send to finish for ``event``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = next((r for r in replies if r["inbound"] is event), None)
        if record and record.get("done"):
            return record
        await asyncio.sleep(0.1)
    raise TimeoutError(f"no reply recorded for message {event.message_id}")


def check(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    log(f"{'PASS' if ok else 'FAIL'} {name}: {detail}")


async def main() -> int:
    global adapter

    config = PlatformConfig(
        enabled=True,
        extra={
            "base_url": BASE_URL,
            "workspace_id": WORKSPACE_ID,
            "email": BOT_EMAIL,
            "password": BOT_PASSWORD,
            "poll_interval": 1.0,
            "use_websocket": os.environ.get("ORBIT_USE_WEBSOCKET", "false").lower() == "true",
        },
    )
    adapter = orbit.OrbitAdapter(config)
    adapter.handle_message = handler  # stand-in for the gateway handoff

    if not await adapter.connect():
        log("adapter did not connect")
        return 2
    log(f"connected as {adapter._self_name} ({adapter._self_id}) workspace {WORKSPACE_ID}")

    peer = orbit.OrbitAPI(
        BASE_URL, WORKSPACE_ID, email=PEER_EMAIL, password=PEER_PASSWORD
    )
    await peer.login()
    peer_me = await peer.me()
    log(f"peer signed in as {peer_me['display_name']} ({peer_me['id']})")

    conversation_id = os.environ.get("ORBIT_TEST_CHANNEL", "")
    if not conversation_id:
        conversations = await peer.list_conversations()
        channel = next((c for c in conversations if c["kind"] == "public" and not c["archived"]), None)
        if channel is None:
            log("no public channel in the workspace")
            return 2
        conversation_id = channel["id"]
    log(f"using channel {conversation_id}")

    try:
        # ── 1. channel mention (auto-thread) ────────────────────────────────
        token = uuid.uuid4().hex[:8]
        posted = await peer.send_message(conversation_id, f"<@{adapter._self_id}> channel ping {token}")
        event = await wait_for(
            lambda e: token in e.text, what="channel mention"
        )
        reply = await reply_for(event)
        check(
            "channel mention opens a thread",
            event.source.chat_type == "thread",
            f"msg {posted['id']} type={event.source.chat_type}",
        )
        check(
            "channel thread is rooted at the inbound message",
            event.source.thread_id == posted["id"],
            f"thread={event.source.thread_id} root={posted['id']}",
        )
        check("channel reply", reply["success"], f"msg {reply['message_id']}")

        page = await peer.get_thread(posted["id"], limit=50)
        check(
            "channel reply lands in the thread",
            any(m["id"] == reply["message_id"] for m in page.get("items", [])),
            f"{reply['message_id']} in thread {posted['id']}",
        )

        # ── 2. thread round-trip ────────────────────────────────────────────
        root_token = uuid.uuid4().hex[:8]
        root = await peer.send_message(
            conversation_id, f"<@{adapter._self_id}> thread root {root_token}"
        )
        root_event = await wait_for(lambda e: root_token in e.text, what="thread root")
        root_reply = await reply_for(root_event)
        check(
            "channel root answer auto-threaded",
            root_event.source.thread_id == root["id"],
            f"thread={root_event.source.thread_id} root={root['id']}",
        )
        root_page = await peer.get_thread(root["id"], limit=50)
        check(
            "root answer lands in the thread",
            any(m["id"] == root_reply["message_id"] for m in root_page.get("items", [])),
            f"{root_reply['message_id']} in thread {root['id']}",
        )
        # The peer now opens a thread on the ROOT (the thread the bot already answers in).
        follow = await peer.send_message(
            conversation_id, f"<@{adapter._self_id}> thread follow-up {root_token}",
            thread_root_id=root["id"],
        )
        thread_event = await wait_for(
            lambda e: root_token in e.text and e.source.thread_id == root["id"],
            what="thread follow-up",
        )
        thread_reply = await reply_for(thread_event)
        check("thread mapping", thread_event.source.chat_type == "thread", f"root {root['id']}")
        check("thread reply", thread_reply["success"], f"msg {thread_reply['message_id']}")

        page = await peer.get_thread(root["id"], limit=50)
        check(
            "thread reply visible in thread",
            any(m["id"] == thread_reply["message_id"] for m in page.get("items", [])),
            f"{thread_reply['message_id']} in thread {root['id']}",
        )

        # Once the bot has answered in the thread it follows it: no further mention needed.
        continued = f"thread continued {root_token}"
        await peer.send_message(conversation_id, continued, thread_root_id=root["id"])
        second = await wait_for(
            lambda e: e.text == continued and e.source.thread_id == root["id"],
            what="unmentioned thread follow-up",
        )
        second_reply = await reply_for(second)
        check("thread follow without mention", second_reply["success"], f"msg {second_reply['message_id']}")
        _ = root_event, follow

        # ── 3. DM round-trip ────────────────────────────────────────────────
        dm = await peer.open_dm([adapter._self_id])
        dm_token = uuid.uuid4().hex[:8]
        await peer.send_message(dm["id"], f"dm ping {dm_token}")
        dm_event = await wait_for(lambda e: dm_token in e.text, what="dm")
        dm_reply = await reply_for(dm_event)
        check("dm mapping", dm_event.source.chat_type == "dm", f"conversation {dm['id']}")
        check("dm reply", dm_reply["success"], f"msg {dm_reply['message_id']}")

        head = await peer.list_messages(dm["id"], limit=10)
        check(
            "dm reply visible",
            any(m["id"] == dm_reply["message_id"] for m in head),
            f"{dm_reply['message_id']} present in DM",
        )
    except TimeoutError as exc:
        check("round-trip", False, str(exc))
    finally:
        await peer.close()
        await adapter.disconnect()

    failed = [name for name, ok, _ in results if not ok]
    log(f"{len(results) - len(failed)}/{len(results)} checks passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
