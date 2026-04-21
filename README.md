# jarvis-recon-bot

Discord bridge into the [jarvis-recon](https://github.com/leronwilliams/jarvis-recon)
task queue. Lives in the Jarvis server's `#jarvis-hq` channel, accepts
`!`-prefixed commands from a whitelist of user IDs, inserts task rows
into the existing Neon `jarvis_recon_tasks` table with
`origin='discord-bot'`, polls for completion, and posts the result back.

## What this is not

- **Not a new source of authority.** Every task still passes through
  the jarvis-recon agent's `lib/sql_safety`, `lib/redactor`, and
  `lib/whitelist`. The bot only *queues* work the agent was already
  willing to do.
- **Not a process manager.** The bot never touches SIS, SurGard, SQL
  Server, or the monitoring PC.
- **Not a dispatcher.** Life-safety signal handling is entirely in the
  SIS Alarm Center — the bot is reporting-only.

## Commands

| Command | Effect |
|---------|--------|
| `!help` | List commands |
| `!status` | Last agent heartbeat within 5 min + service state |
| `!report today` | Signals in the last 24h |
| `!report week` | Signals in the last 7 days |
| `!report health` | Host + stack health snapshot |
| `!report account <id>` | 30-day history for one account |
| `!query <SQL>` | Read-only query (safety-gated by the agent) |

Results over 1500 chars are posted as a `.txt` attachment.

## Authorization

Two gates, both configured in `config.ini`:

1. `[discord] allowed_channel_id` — single channel ID. The bot ignores
   messages from any other channel.
2. `[discord] allowed_user_ids` — comma-separated Discord user IDs.
   Blank = nobody authorized; any command attempt logs the attempting
   user's ID to stderr so you can copy it into config on first boot.

Every accepted command writes a `discord_bot.cmd` row to
`jarvis_recon_audit`.

## Install (VPS)

```bash
mkdir -p /opt/jarvis-recon-bot && cd /opt/jarvis-recon-bot
git clone https://github.com/leronwilliams/jarvis-recon-bot.git .

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Copy credentials from the Jarvis PC (scp from your workstation):
#   scp ~/.openclaw/credentials/discord/jarvis-recon-bot-token.txt \
#       root@srv1598913:/opt/jarvis-recon-bot/credentials/discord-bot.token
#   scp ~/.openclaw/credentials/neon/jarvis-recon-conn.json \
#       root@srv1598913:/opt/jarvis-recon-bot/credentials/neon.json
chmod 600 credentials/*

cp config.example.ini config.ini
# Fill in allowed_channel_id (see "How to get the channel ID" below)
# Leave allowed_user_ids empty on first boot.

cp systemd/jarvis-recon-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now jarvis-recon-bot
journalctl -u jarvis-recon-bot -f
```

### How to get the channel ID

Discord → User Settings → Advanced → **Developer Mode: on**. Then
right-click `#jarvis-hq` → **Copy Channel ID**. Paste into
`[discord] allowed_channel_id`.

### First-run authorization

Type any command in `#jarvis-hq`. The bot will reply "🔒 Not authorized"
and log the attempting user's ID to journalctl:

```
Unauthorized user: <name> (id: 123456789012345678) attempted command '!status'
```

Copy the ID into `[discord] allowed_user_ids` and `systemctl restart
jarvis-recon-bot`.

## Adding a new command

1. Extend `command_parser.py` with a new branch in `parse()` and a
   matching entry in `KNOWN_COMMANDS`.
2. Add a parser test case in `tests/test_command_parser.py`.
3. If the command queues a task: extend `_dispatch_*` in `bot.py` and
   (if needed) register the new `task_type` in the jarvis-recon agent's
   `lib/whitelist.py`.
4. `pytest -q` must stay green before commit.

## Schema migration

On every start the bot idempotently runs:

```sql
ALTER TABLE jarvis_recon_tasks
    ADD COLUMN IF NOT EXISTS posted_to_discord BOOLEAN DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_jrt_discord_pending
    ON jarvis_recon_tasks (origin, status, posted_to_discord)
    WHERE origin = 'discord-bot' AND posted_to_discord = FALSE;
```

Safe on every restart. Lets the bot find tasks it hasn't yet posted
back to Discord without scanning the whole table.

## License

MIT. © 2026 Leron Williams / Formartiq Limited.
