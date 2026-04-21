"""Parse !-prefixed chat commands into (command, subcommand, args).

Kept separate from bot.py so the parsing rules are exercised without
importing discord.py in the test suite. All functions here are pure.
"""
import re
from dataclasses import dataclass


class ParseError(ValueError):
    """Raised when the command line is syntactically invalid."""


class NotACommand(Exception):
    """Raised when the message doesn't start with the command prefix.
    Signals to the bot that the message should be silently ignored."""


PREFIX = "!"
KNOWN_COMMANDS = {"help", "status", "report", "query"}
KNOWN_REPORT_SUBCOMMANDS = {"today", "week", "health", "falsealarm", "account"}

# Maps a !query SQL statement to the whitelisted task_type the agent will
# dispatch it under. We only sniff the dominant FROM-clause table.
_TRAFFIC_HINTS = ("TRAFFIC.", "dbo.Incoming", "UserLogins", "Signal")


@dataclass
class ParsedCommand:
    command: str
    subcommand: str | None
    args: dict

    # Provided for older test expectations that unpack a 3-tuple.
    def __iter__(self):
        yield self.command
        yield self.subcommand
        yield self.args


def _strip_prefix(raw: str) -> str:
    s = raw.strip()
    if not s.startswith(PREFIX):
        raise NotACommand(raw)
    return s[len(PREFIX):].strip()


def _reject_if_multistatement(sql: str) -> None:
    """Cheap pre-check; the agent's lib.sql_safety is the real gate.
    We pre-reject obvious multi-statement SQL so the user gets fast feedback."""
    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        raise ParseError("SQL contains stacked statements (';' not allowed)")


def _route_query(sql: str) -> str:
    """Pick a whitelisted sql.query_* task_type from a FROM-clause sniff."""
    haystack = sql.upper()
    if any(h.upper() in haystack for h in _TRAFFIC_HINTS):
        return "sql.query_traffic"
    return "sql.query_subscriber"


def parse(raw_message: str) -> ParsedCommand:
    """Parse a raw chat message. Raises NotACommand for non-commands and
    ParseError for malformed commands."""
    body = _strip_prefix(raw_message)
    if not body:
        raise ParseError("empty command")

    tokens = body.split(None, 2)  # at most 3 parts: cmd, sub, rest
    cmd = tokens[0].lower()

    if cmd not in KNOWN_COMMANDS:
        raise ParseError(f"unknown command: {cmd!r}")

    if cmd in ("help", "status"):
        return ParsedCommand(cmd, None, {})

    if cmd == "report":
        if len(tokens) < 2:
            raise ParseError("usage: !report today|week|health|falsealarm|account <id>")
        sub = tokens[1].lower()
        if sub not in KNOWN_REPORT_SUBCOMMANDS:
            raise ParseError(
                f"unknown report: {sub!r}. "
                "try: today | week | health | falsealarm | account <id>"
            )
        if sub == "account":
            rest = tokens[2] if len(tokens) > 2 else ""
            m = re.fullmatch(r"\s*(\d+)\s*", rest or "")
            if not m:
                raise ParseError("account id must be a positive integer")
            n = int(m.group(1))
            if n <= 0:
                raise ParseError("account id must be a positive integer")
            return ParsedCommand("report", "account", {"account_id": n,
                                                        "days": 30})
        return ParsedCommand("report", sub, {})

    # cmd == "query"
    sql = tokens[1] if len(tokens) > 1 else ""
    if len(tokens) > 2:
        sql = sql + " " + tokens[2]
    sql = sql.strip()
    if not sql:
        raise ParseError("usage: !query <SQL>")
    _reject_if_multistatement(sql)
    task_type = _route_query(sql)
    return ParsedCommand("query", None, {"query": sql, "task_type": task_type})
