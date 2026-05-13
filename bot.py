"""jarvis-recon-bot — Discord bridge to the jarvis-recon task queue.

Reads !-prefixed commands from a single whitelisted channel, inserts
rows into jarvis_recon_tasks (origin='discord-bot'), polls for results,
and posts them back to the originating message.

The bot adds no authority — every task still passes through the
jarvis-recon agent's sql_safety / redactor / whitelist gates before
execution.
"""
from __future__ import annotations

import asyncio
import configparser
import io
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import discord
import psycopg2
import psycopg2.extras

from command_parser import (
    KNOWN_COMMANDS,
    NotACommand,
    ParseError,
    ParsedCommand,
    parse,
)


CONFIG_PATH = os.environ.get("JRB_CONFIG", "/opt/jarvis-recon-bot/config.ini")
OBSERVATORY_EMIT = os.environ.get(
    "OBSERVATORY_EMIT", "/root/observatory/hooks/observatory-emit.sh")


def _emit_sync(event_type: str, title: str, body: str, level: str,
               trace_id: str, tool: str) -> None:
    """Blocking emit — never raises. Run via asyncio.to_thread from async paths."""
    if not os.path.isfile(OBSERVATORY_EMIT):
        return
    env = os.environ.copy()
    if trace_id:
        env["OPENCLAW_TRACE_ID"] = trace_id
    if tool:
        env["OPENCLAW_TOOL"] = tool
    try:
        subprocess.run(
            [OBSERVATORY_EMIT, event_type, title[:200], (body or "")[:4000], level],
            env=env, timeout=5, check=False,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def emit(event_type: str, title: str, *, body: str = "", level: str = "info",
         trace_id: str = "", tool: str = "") -> None:
    """Fire-and-forget observatory event. Won't block the bot's event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _emit_sync(event_type, title, body, level, trace_id, tool)
        return
    loop.create_task(asyncio.to_thread(
        _emit_sync, event_type, title, body, level, trace_id, tool))


# ---------------------------------------------------------------------------
# Config + credentials
# ---------------------------------------------------------------------------


def _load_config(path: str = CONFIG_PATH) -> configparser.ConfigParser:
    if not os.path.isfile(path):
        raise SystemExit(f"config not found: {path}")
    cp = configparser.ConfigParser()
    cp.read(path, encoding="utf-8")
    return cp


def _read_token(token_file: str) -> str:
    with open(token_file, encoding="utf-8") as f:
        token = f.read().strip()
    if not token:
        raise SystemExit(f"bot token file empty: {token_file}")
    return token


def _read_neon_uri(creds_file: str) -> str:
    with open(creds_file, encoding="utf-8") as f:
        data = json.load(f)
    try:
        return data["connection_uris"][0]["connection_uri"]
    except (KeyError, IndexError) as e:
        raise SystemExit(f"neon credentials malformed: {e}") from e


# ---------------------------------------------------------------------------
# Neon schema migration + helpers
# ---------------------------------------------------------------------------


MIGRATION_SQL = """
ALTER TABLE jarvis_recon_tasks
    ADD COLUMN IF NOT EXISTS posted_to_discord BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_jrt_discord_pending
    ON jarvis_recon_tasks (origin, status, posted_to_discord)
    WHERE origin = 'discord-bot' AND posted_to_discord = FALSE;
"""


def _run_migration(neon_uri: str) -> None:
    conn = psycopg2.connect(neon_uri)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(MIGRATION_SQL)
    finally:
        conn.close()


def _audit_cmd(neon_uri: str, user_id: int, user_name: str, cmd_text: str,
               channel_id: int, message_id: int) -> None:
    """Write a discord_bot.cmd row to jarvis_recon_audit. Best-effort."""
    ctx = {
        "user_id": str(user_id),
        "user_name": user_name,
        "command": cmd_text[:500],
        "channel_id": str(channel_id),
        "message_id": str(message_id),
    }
    try:
        conn = psycopg2.connect(neon_uri)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jarvis_recon_audit "
                    "(ts, agent_host, event_type, severity, message, context) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (datetime.now(timezone.utc), "jarvis-recon-bot",
                     "discord_bot.cmd", "info",
                     f"Discord cmd from {user_name}: {cmd_text[:80]}",
                     json.dumps(ctx)),
                )
        finally:
            conn.close()
    except Exception as e:
        logging.warning("audit_cmd failed: %s", e)


def _enqueue_task(neon_uri: str, task_type: str, payload: dict,
                  channel_id: int, message_id: int) -> str:
    """Insert a task row. Returns the new task id (uuid str)."""
    full_payload = dict(payload)
    full_payload["_discord"] = {
        "channel_id": str(channel_id),
        "message_id": str(message_id),
    }
    conn = psycopg2.connect(neon_uri)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO jarvis_recon_tasks "
                "(task_type, task_payload, status, origin) "
                "VALUES (%s, %s, 'pending', 'discord-bot') "
                "RETURNING id",
                (task_type, json.dumps(full_payload)),
            )
            return str(cur.fetchone()[0])
    finally:
        conn.close()


def _fetch_task(neon_uri: str, task_id: str) -> dict | None:
    conn = psycopg2.connect(neon_uri,
                            cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, status, result, error, task_payload "
                "FROM jarvis_recon_tasks WHERE id=%s", (task_id,))
            row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _mark_posted(neon_uri: str, task_id: str) -> None:
    conn = psycopg2.connect(neon_uri)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jarvis_recon_tasks "
                "SET posted_to_discord=TRUE WHERE id=%s", (task_id,))
    finally:
        conn.close()


def _latest_agent_heartbeat(neon_uri: str) -> dict | None:
    """Pull the most recent heartbeat/started row within the last 5 min."""
    conn = psycopg2.connect(neon_uri,
                            cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts, agent_host, event_type, message "
                "FROM jarvis_recon_audit "
                "WHERE event_type IN ('heartbeat', 'agent.started') "
                "AND ts > now() - interval '5 minutes' "
                "ORDER BY ts DESC LIMIT 1"
            )
            row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Discord client
# ---------------------------------------------------------------------------


HELP_TEXT = (
    "**jarvis-recon-bot commands**\n"
    "```\n"
    "!help                         Show this help\n"
    "!status                       Agent heartbeat + service state\n"
    "!report today                 Signals today (operator-local time)\n"
    "!report week                  Signals in the last 7 local days\n"
    "!report health                Host + stack health snapshot\n"
    "!report falsealarm            False-alarm history (disabled stub)\n"
    "!report account <id>          30-day history for one AcctNum\n"
    "!query <SQL>                  Run a read-only query (safety gated)\n"
    "```"
)


def _format_report_result(result: dict) -> str:
    """Report handlers return {ok, markdown, csv}. Prefer markdown."""
    md = (result or {}).get("markdown") or ""
    if not md:
        return f"```json\n{json.dumps(result or {}, indent=2, default=str)[:1800]}\n```"
    return md if len(md) < 1900 else md[:1900] + "\n…(truncated)"


def _format_query_result(result: dict) -> str:
    """sql.query_* returns {ok, row_count, rows, table_context} or
    {ok: False, rejected: True, reason}."""
    if not result:
        return "⚠️ Empty result from agent."
    if result.get("rejected"):
        return f"🛑 Query rejected by safety gate: `{result.get('reason', 'unknown')}`"
    if not result.get("ok"):
        return f"❌ Query failed: `{result}`"
    rows = result.get("rows", [])
    if not rows:
        return f"✅ Query ok — 0 rows (table context: `{result.get('table_context', '')}`)"
    header = list(rows[0].keys())
    lines = [" | ".join(header), "-" * (sum(len(h) for h in header) + 3 * (len(header) - 1))]
    for r in rows[:50]:
        lines.append(" | ".join(str(r.get(h, ""))[:40] for h in header))
    table = "\n".join(lines)
    if len(rows) > 50:
        table += f"\n…({len(rows) - 50} more rows)"
    return table


class JarvisReconBot(discord.Client):
    def __init__(self, cfg: configparser.ConfigParser, neon_uri: str,
                 bot_token: str):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.cfg = cfg
        self.neon_uri = neon_uri
        self.bot_token = bot_token
        self.allowed_channel_id = int(cfg["discord"].get("allowed_channel_id") or 0)
        raw_users = cfg["discord"].get("allowed_user_ids") or ""
        self.allowed_user_ids = {
            int(u.strip()) for u in raw_users.split(",") if u.strip().isdigit()
        }
        self.poll_interval = int(cfg["bot"].get("poll_interval_seconds", "5"))
        self.slow_threshold = int(cfg["bot"].get("slow_task_threshold_seconds", "60"))
        self.task_timeout = int(cfg["bot"].get("task_timeout_seconds", "300"))

    # -- lifecycle -------------------------------------------------------

    async def on_ready(self):
        await self.change_presence(activity=discord.Activity(
            type=discord.ActivityType.watching, name="jarvis-recon"))
        logging.info("Connected as %s (id=%s)", self.user, self.user.id)
        emit("prompt", "recon bot online",
             body=f"connected as {self.user} (id={self.user.id})")
        if self.allowed_channel_id:
            ch = self.get_channel(self.allowed_channel_id)
            if ch:
                try:
                    await ch.send("🟢 Jarvis-Recon-Bot online — type !help for commands")
                except Exception as e:
                    logging.warning("hello-post failed: %s", e)

    async def shutdown_message(self):
        if not self.allowed_channel_id:
            return
        ch = self.get_channel(self.allowed_channel_id)
        if ch:
            try:
                await ch.send("🔴 Jarvis-Recon-Bot stopping — see you on the other side")
            except Exception:
                pass

    # -- authorization ---------------------------------------------------

    def _is_authorized_channel(self, channel_id: int) -> bool:
        return self.allowed_channel_id and channel_id == self.allowed_channel_id

    def _is_authorized_user(self, user_id: int) -> bool:
        return user_id in self.allowed_user_ids

    # -- message handling ------------------------------------------------

    async def on_message(self, msg: discord.Message):
        if msg.author.bot:
            return
        if not self._is_authorized_channel(msg.channel.id):
            return
        try:
            parsed = parse(msg.content)
        except NotACommand:
            return
        except ParseError as e:
            emit("error", "command parse error",
                 body=f"input: {msg.content[:200]}\nerror: {e}",
                 level="warn")
            await msg.reply(f"⚠️ {e}")
            return

        if not self._is_authorized_user(msg.author.id):
            sys.stderr.write(
                f"Unauthorized user: {msg.author.name} (id: {msg.author.id}) "
                f"attempted command {msg.content[:80]!r}\n"
            )
            sys.stderr.flush()
            emit("error", "unauthorized user",
                 body=f"user={msg.author} ({msg.author.id}) cmd={msg.content[:120]}",
                 level="warn")
            await msg.reply("🔒 Not authorized. Ask Ron to add your user ID.")
            return

        _audit_cmd(self.neon_uri, msg.author.id, str(msg.author),
                   msg.content, msg.channel.id, msg.id)

        trace = uuid.uuid4().hex[:12]
        emit("prompt", f"!{parsed.command}" + (f" {parsed.subcommand}" if parsed.subcommand else ""),
             body=f"from {msg.author} in channel {msg.channel.id}\nraw: {msg.content[:300]}",
             trace_id=trace)

        if parsed.command == "help":
            await msg.reply(HELP_TEXT)
            emit("reply", "help posted", trace_id=trace)
            return
        if parsed.command == "status":
            await self._handle_status(msg, trace_id=trace)
            return
        if parsed.command == "report":
            await self._dispatch_report(msg, parsed, trace_id=trace)
            return
        if parsed.command == "query":
            await self._dispatch_query(msg, parsed, trace_id=trace)
            return

    # -- command handlers ------------------------------------------------

    async def _handle_status(self, msg: discord.Message, *, trace_id: str = ""):
        try:
            hb = await asyncio.to_thread(_latest_agent_heartbeat, self.neon_uri)
        except Exception as e:
            emit("error", "status check failed", body=str(e),
                 level="error", trace_id=trace_id)
            await msg.reply(f"❌ Status check failed: `{e}`")
            return
        if not hb:
            emit("reply", "no agent heartbeat (>5m)",
                 body="monitoring PC may be down", level="warn", trace_id=trace_id)
            await msg.reply(
                "⚠️ No agent heartbeat in the last 5 minutes. "
                "The jarvis-recon service on the monitoring PC may be down."
            )
            return
        now = datetime.now(timezone.utc)
        age = int((now - hb["ts"]).total_seconds())
        emit("reply", f"heartbeat OK ({age}s ago)",
             body=f"host={hb['agent_host']} type={hb['event_type']}\n{hb['message'][:300]}",
             trace_id=trace_id)
        await msg.reply(
            f"🟢 Agent `{hb['agent_host']}` last heartbeat {age}s ago\n"
            f"> `{hb['event_type']}`: {hb['message'][:180]}"
        )

    async def _dispatch_report(self, msg: discord.Message, parsed: ParsedCommand,
                               *, trace_id: str = ""):
        if parsed.subcommand == "today":
            task_type = "report.today"
            payload = {}
        elif parsed.subcommand == "week":
            task_type = "report.week"
            payload = {}
        elif parsed.subcommand == "health":
            task_type = "report.health"
            payload = {}
        elif parsed.subcommand == "falsealarm":
            task_type = "report.falsealarm"
            payload = {}
        elif parsed.subcommand == "account":
            task_type = "report.account"
            payload = dict(parsed.args)
        else:
            emit("error", f"unknown report subcommand: {parsed.subcommand}",
                 level="warn", trace_id=trace_id)
            await msg.reply(f"⚠️ unknown report subcommand: {parsed.subcommand}")
            return
        await self._enqueue_and_wait(msg, task_type, payload,
                                     formatter=_format_report_result,
                                     trace_id=trace_id)

    async def _dispatch_query(self, msg: discord.Message, parsed: ParsedCommand,
                              *, trace_id: str = ""):
        task_type = parsed.args.pop("task_type")
        await self._enqueue_and_wait(msg, task_type, parsed.args,
                                     formatter=_format_query_result,
                                     trace_id=trace_id)

    async def _enqueue_and_wait(self, msg, task_type, payload, formatter,
                                *, trace_id: str = ""):
        try:
            task_id = await asyncio.to_thread(
                _enqueue_task, self.neon_uri, task_type, payload,
                msg.channel.id, msg.id,
            )
        except Exception as e:
            emit("error", "queue insert failed", body=str(e),
                 level="error", trace_id=trace_id, tool=task_type)
            await msg.reply(f"❌ Queue insert failed: `{e}`")
            return
        emit("tool_use", f"task enqueued: {task_type}",
             body=f"task_id={task_id} payload={json.dumps(payload)[:300]}",
             trace_id=trace_id, tool=task_type)

        status = await msg.reply(
            f"⏳ Queued `{task_type}` (id: `{task_id[:8]}…`)")
        start = time.time()
        slow_edited = False

        while True:
            await asyncio.sleep(self.poll_interval)
            try:
                row = await asyncio.to_thread(_fetch_task, self.neon_uri, task_id)
            except Exception as e:
                emit("error", "task poll failed", body=str(e),
                     level="error", trace_id=trace_id, tool=task_type)
                await status.edit(content=f"❌ Poll failed: `{e}`")
                return
            if row and row["status"] in ("completed", "failed"):
                result = row.get("result") or {}
                error = row.get("error")
                try:
                    await asyncio.to_thread(_mark_posted, self.neon_uri, task_id)
                except Exception:
                    pass
                if row["status"] == "failed":
                    emit("error", f"task failed: {task_type}",
                         body=f"error={error}\nelapsed={int(time.time()-start)}s",
                         level="error", trace_id=trace_id, tool=task_type)
                    await status.edit(content=f"❌ `{task_type}` failed: `{error}`")
                    return
                body = formatter(result)
                elapsed_s = int(time.time() - start)
                emit("reply", f"task complete: {task_type} ({elapsed_s}s)",
                     body=body, trace_id=trace_id, tool=task_type)
                if len(body) > 1500:
                    buf = io.BytesIO(body.encode("utf-8"))
                    await status.edit(content=f"✅ `{task_type}` complete (attached)")
                    await msg.channel.send(
                        file=discord.File(buf, filename=f"{task_type}.txt"))
                else:
                    await status.edit(
                        content=f"✅ `{task_type}` complete\n```\n{body[:1800]}\n```")
                return

            elapsed = time.time() - start
            if elapsed > self.task_timeout:
                emit("error", f"task timeout: {task_type}",
                     body=f"timed out after {self.task_timeout}s (task_id={task_id})",
                     level="error", trace_id=trace_id, tool=task_type)
                await status.edit(
                    content=f"⏰ `{task_type}` timed out after {self.task_timeout}s "
                            f"(id: `{task_id[:8]}…`). Agent may be overloaded.")
                return
            if elapsed > self.slow_threshold and not slow_edited:
                slow_edited = True
                await status.edit(
                    content=f"⏳ Still working on `{task_type}` "
                            f"(taking longer than usual)…")

    # -- run helper ------------------------------------------------------

    def run_forever(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def _stop(*_):
            logging.info("Shutdown signal received.")
            async def _bye():
                try:
                    await self.shutdown_message()
                finally:
                    await self.close()
            asyncio.run_coroutine_threadsafe(_bye(), loop)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _stop)
            except (NotImplementedError, RuntimeError):
                # Windows: asyncio loop doesn't support add_signal_handler.
                signal.signal(sig, lambda *_: _stop())

        try:
            loop.run_until_complete(self.start(self.bot_token))
        finally:
            loop.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = _load_config()
    token = _read_token(cfg["discord"]["token_file"])
    neon_uri = _read_neon_uri(cfg["neon"]["credentials_file"])

    logging.info("Running Neon migration (idempotent)…")
    _run_migration(neon_uri)

    bot = JarvisReconBot(cfg, neon_uri, token)
    bot.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
