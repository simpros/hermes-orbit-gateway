# Hermes Orbit Gateway

An [Orbit](https://github.com/coollabsio/orbit) chat adapter for [Hermes Agent](https://hermes-agent.nousresearch.com).
Hermes joins a workspace as a regular member and answers in **channels, threads and DMs** —
the same reply surface it has on Discord.

This is a Hermes **messaging plugin**: `plugin.yaml` + `adapter.py`, registered through
`ctx.register_platform()`. No core Hermes changes, no patch to Orbit.

```
Orbit  ──poll (source of truth)──▶  OrbitAdapter  ──▶  Hermes gateway ──▶ agent
   ▲                                    │
   └───── live websocket (latency) ─────┘
```

## Why polling *and* a websocket

Orbit's chat API is **session-only**: `POST /api/v1/auth/login` returns a cookie (or a bearer
token for native clients) and every chat route, plus `GET .../live`, is judged by it. There is no
API-token path for chat writes and no bot webhook. So the adapter:

1. signs in with email/password (or a pre-issued session cookie) and keeps the cookie,
2. polls `GET .../conversations/{id}/messages?after=<cursor>` per conversation — **this is the
   source of truth** — and `GET .../threads/{root}` for threads the bot follows or that moved,
3. optionally subscribes to the `/live` websocket for near-instant delivery. A dropped socket
   costs latency only: it never advances the poll cursors, and duplicates are deduplicated by
   message id.

Both paths feed one dispatcher, so a message is delivered exactly once.

## Requirements

- Hermes Agent with plugin support (any recent build).
- An Orbit instance and a workspace.
- A dedicated **bot account** in that workspace (email + password, or an existing session cookie).
  Give it access to the channels it should answer in; private channels need an explicit invite.

## Install

```bash
# 1. drop the plugin into the Hermes plugin directory
git clone https://github.com/simpros/hermes-orbit-gateway \
  "${HERMES_HOME:-$HOME/.hermes}/plugins/orbit-gateway"

# 2. enable it
hermes plugins enable orbit-platform

# 3. credentials go in .env (never in config.yaml, never in git)
cat >> "${HERMES_HOME:-$HOME/.hermes}/.env" <<'EOF'
ORBIT_BASE_URL=https://orbit.example.com
ORBIT_WORKSPACE_ID=01a1a7d4-514c-72b1-a95e-2036e82cb14e
ORBIT_EMAIL=hermes@example.com
ORBIT_PASSWORD=…
ORBIT_HOME_CHANNEL=01a1a7d4-5144-7a5d-8c7b-a676f224894f
EOF

# 4. restart the gateway
hermes gateway restart
```

From a chat you can also send `/restart` (allowed on this platform) to pick up config or code
changes without leaving the session — the gateway reconnects and keeps polling.

Verify with `hermes gateway status`: the platform is listed as `Orbit` when `ORBIT_BASE_URL` and
`ORBIT_WORKSPACE_ID` are set **and** a credential (`ORBIT_SESSION_COOKIE`, or `ORBIT_EMAIL` +
`ORBIT_PASSWORD`) is present.

## Configuration

Env wins over `config.yaml`, and both are read through Hermes' profile-scoped helpers, so a
multiplexed profile only ever sees its own values.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `ORBIT_BASE_URL` | yes | — | Instance base URL, e.g. `https://orbit.example.com` |
| `ORBIT_WORKSPACE_ID` | yes | — | Workspace UUID (from the workspace URL) |
| `ORBIT_EMAIL` | one of | — | Bot account email |
| `ORBIT_PASSWORD` | one of | — | Bot account password |
| `ORBIT_SESSION_COOKIE` | one of | — | `name=value` cookie, or a bare session token (sent as `Authorization: Bearer`) |
| `ORBIT_SESSION_COOKIE_NAME` | no | `__Host-orbit_session` (`orbit_session_dev` on loopback) | Cookie name for a bare token |
| `ORBIT_HOME_CHANNEL` | no | first writable channel | Conversation id for cron / `deliver=orbit` |
| `ORBIT_REQUIRE_MENTION` | no | `true` | Only answer channel messages that address the bot |
| `ORBIT_AUTO_THREAD` | no | `true` | Answer each channel mention in its own thread instead of inline |
| `ORBIT_NO_THREAD_CHANNELS` | no | — | Comma-separated conversation ids that keep answering inline |
| `ORBIT_MENTION_NAME` | no | display name + its first word | Extra name that addresses the bot (`@hermes`) |
| `ORBIT_POLL_INTERVAL` | no | `3` | Seconds between polls |
| `ORBIT_USE_WEBSOCKET` | no | `true` | Use `/live` for low latency |
| `ORBIT_ALLOWED_USERS` | no | — | Comma-separated Orbit user ids allowed to talk to the bot |
| `ORBIT_ALLOW_ALL_USERS` | no | `false` | Allow every workspace member (dev only) |

The same settings can live in `config.yaml`; the adapter bridges them to env at load time:

```yaml
gateway:
  platforms:
    orbit:
      enabled: true
      extra:
        base_url: https://orbit.example.com
        workspace_id: 01a1a7d4-514c-72b1-a95e-2036e82cb14e
        require_mention: true
        auto_thread: true
        poll_interval: 3
        use_websocket: true
        # credentials: prefer .env
```

## Behaviour

- **Channels / private channels** — answered when the message mentions the bot (`<@user_id>` in
  the body, `@hermes`, or a reply to one of the bot's messages). Set
  `ORBIT_REQUIRE_MENTION=false` to answer everything in channels.
- **Auto-thread (default on)** — a channel mention does not dump its answer into the channel:
  the incoming message becomes the thread root, the bot replies with `thread_root_id` set to it,
  and the thread's follow-ups stay there. Add a conversation id to `ORBIT_NO_THREAD_CHANNELS`
  (or set `ORBIT_AUTO_THREAD=false`) to answer inline instead.
- **Threads** — a thread reply is dispatched with the thread as its session and answered *into the
  thread*; once the bot has replied in a thread it answers follow-ups there without a mention.
- **DMs** — always answered; a DM conversation id is just another `chat_id`.
- **Long answers** — split at 4000 characters (Orbit's body cap) with `(1/3)` markers, fence-aware
  through Hermes' `truncate_message`.
- **Attachments** — `send_image` / `send_image_file` / `send_document` upload through
  `POST .../chat/files` and attach the file id to the message; inbound attachments appear as a
  text placeholder.
- **Typing** — a `/live` socket frame when the websocket is up, a no-op otherwise.
- **Reconnect** — expired session (`401`) triggers one transparent re-login + retry; websocket
  drops back off exponentially while polling continues.
- **Cron** — `cronjob(deliver="orbit")` sends through `ORBIT_HOME_CHANNEL`, including from a
  standalone process via `standalone_sender_fn`.

## Security

- Session cookies and passwords are read from the profile's `.env`, through
  `get_scoped_secret`, and are never logged.
- The adapter never posts as a human: use a dedicated account, and keep `ORBIT_ALLOWED_USERS` set
  in shared workspaces (`ORBIT_ALLOW_ALL_USERS` is for throwaway dev instances).
- Orbit enforces CSRF by `Origin`; the adapter sends the origin derived from `ORBIT_BASE_URL` on
  every request, so it works behind the same reverse proxy a browser uses.
- Two-factor accounts cannot sign in with a password — issue a session cookie elsewhere and set
  `ORBIT_SESSION_COOKIE`.

## Limitations

- Chat is session-auth only; there is no bot token or webhook to subscribe to.
- Orbit threads have no name field and no thread-rename route: an auto-thread is identified by
  its root message, and that message's text is the only name shown. Discord's semantic thread
  rename has no Orbit equivalent, so none is attempted.
- Thread replies in threads the bot neither follows nor was mentioned in are discovered by a
  periodic head scan (every ~30 s), not instantly.
- No reaction, edit or delete surface yet — text, threads, DMs and file uploads only.
- History is not replayed on startup: the adapter starts at the current head of each conversation.

## Development

```bash
pip install -e ".[dev]"   # httpx + pytest + pytest-asyncio + ruff
make check        # ruff + pytest
make e2e          # round-trip against a real Orbit (see scripts/e2e_roundtrip.py)
```

Tests import Hermes (`gateway.platforms.base`). Point `HERMES_AGENT_ROOT` at a checkout, or install
it with `pip install -e`, if it is not already importable.

### Round-trip against a local Orbit

```bash
docker run -d --name orbit --network host \
  -e ORBIT_ENV=development -e ORBIT_BIND=127.0.0.1 -e ORBIT_PORT=19111 \
  -e ORBIT_APP_KEY="$(openssl rand -base64 32)" -v /tmp/orbit-data:/data \
  ghcr.io/coollabsio/orbit:latest
# complete setup at the URL the container logs, then seed two users + a workspace:
docker stop orbit && docker run --rm --network host -v /tmp/orbit-data:/data \
  --entrypoint /orbit ghcr.io/coollabsio/orbit:latest seed && docker start orbit
```

Seeded accounts: `test@example.com` / `member@example.com`, password `password` (development only).
Then:

```bash
ORBIT_BASE_URL=http://127.0.0.1:19111 \
ORBIT_WORKSPACE_ID=<workspace uuid> \
ORBIT_EMAIL=test@example.com ORBIT_PASSWORD=password \
ORBIT_PEER_EMAIL=member@example.com ORBIT_PEER_PASSWORD=password \
python scripts/e2e_roundtrip.py
```

## License

MIT
