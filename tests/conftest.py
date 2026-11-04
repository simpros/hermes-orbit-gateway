"""Shared fixtures: load ``adapter.py`` the way the Hermes plugin loader does.

The loader imports the plugin's entry module from its file path (no package
context), so the tests do the same instead of relying on ``sys.path`` tricks.

Hermes must be importable (``pip install -e <hermes-agent checkout>``).  When it
is not, ``HERMES_AGENT_ROOT`` — or the default ``~/.hermes/hermes-agent`` — is
added to ``sys.path`` so a plain checkout works too.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ADAPTER_PATH = REPO_ROOT / "adapter.py"
MODULE_NAME = "orbit_adapter_under_test"


def _hermes_importable() -> bool:
    try:
        import gateway.platforms.base  # noqa: F401
    except Exception:  # noqa: BLE001 - a missing native sibling counts as "not importable"
        return False
    return True


def _add_hermes_to_path() -> None:
    candidates = [
        os.environ.get("HERMES_AGENT_ROOT", ""),
        os.path.expanduser("~/.hermes/hermes-agent"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_dir() and candidate not in sys.path:
            sys.path.insert(0, candidate)
            if _hermes_importable():
                return


if not _hermes_importable():
    _add_hermes_to_path()

if not _hermes_importable():
    pytest.skip(
        "Hermes Agent is not importable — set HERMES_AGENT_ROOT or pip install -e <hermes-agent>",
        allow_module_level=True,
    )


def _load_adapter() -> types.ModuleType:
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, ADAPTER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def orbit() -> types.ModuleType:
    """The adapter module (imported once per session)."""
    return _load_adapter()


@pytest.fixture()
def adapter(orbit):
    """An adapter with no network, no background tasks, and a known identity."""
    from gateway.config import PlatformConfig

    config = PlatformConfig(
        enabled=True,
        extra={
            "base_url": "https://orbit.example.test",
            "workspace_id": "ws-1",
            "email": "bot@example.test",
            "password": "secret",
            "poll_interval": 60,
            "use_websocket": False,
        },
    )
    instance = orbit.OrbitAdapter(config)
    instance._self_id = "bot-user"
    instance._self_name = "Hermes Bot"
    instance._member_names = {"human-1": "Ada", "bot-user": "Hermes Bot"}
    return instance


@pytest.fixture()
def fake_api(orbit):
    """Factory: a stand-in ``OrbitAPI`` that records sends and can fail on demand."""

    class FakeAPI:
        def __init__(self, result=None, *, error_status=None, error_detail=""):
            self.result = result or {"result": {"id": "0190-x"}}
            self.error_status = error_status
            self.error_detail = error_detail
            self.sent: list[dict] = []

        async def send_message(self, conversation_id, body, **kwargs):
            if self.error_status:
                raise orbit.OrbitAPIError(
                    f"Orbit POST messages -> {self.error_status}: {self.error_detail}",
                    status=self.error_status,
                )
            self.sent.append({"conversation_id": conversation_id, "body": body, **kwargs})
            return dict(self.result.get("result") or {})

        async def upload_file(self, path, *, mime_type=""):
            return {"id": "file-1", "file_name": Path(path).name}

        async def login(self):
            return None

        async def me(self):
            return {"id": "bot-user", "display_name": "Hermes Bot"}

        async def list_conversations(self):
            return []

        async def list_members(self, *, limit=200):
            return []

        def sent_messages(self) -> list[dict]:
            return self.sent

    return FakeAPI
