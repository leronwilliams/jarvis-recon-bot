"""Tests for command_parser — pure, no Discord/psycopg dependencies."""
import pytest

from command_parser import (
    ParsedCommand,
    ParseError,
    NotACommand,
    parse,
)


def test_help_parses():
    p = parse("!help")
    assert (p.command, p.subcommand, p.args) == ("help", None, {})


def test_status_parses():
    p = parse("!status")
    assert (p.command, p.subcommand, p.args) == ("status", None, {})


@pytest.mark.parametrize("sub", ["today", "week", "health"])
def test_report_simple_subcommands(sub):
    p = parse(f"!report {sub}")
    assert p.command == "report"
    assert p.subcommand == sub
    assert p.args == {}


def test_report_account_numeric():
    p = parse("!report account 142")
    assert p.command == "report"
    assert p.subcommand == "account"
    assert p.args == {"account_id": 142, "days": 30}


def test_report_account_non_numeric_rejected():
    with pytest.raises(ParseError):
        parse("!report account abc")


def test_report_account_missing_id_rejected():
    with pytest.raises(ParseError):
        parse("!report account")


def test_report_account_zero_rejected():
    with pytest.raises(ParseError):
        parse("!report account 0")


def test_report_unknown_subcommand():
    with pytest.raises(ParseError):
        parse("!report frobnicate")


def test_query_routes_to_traffic_from_from_clause():
    p = parse("!query SELECT TOP 5 * FROM TRAFFIC.dbo.Incoming")
    assert p.command == "query"
    assert p.args["task_type"] == "sql.query_traffic"
    assert "FROM TRAFFIC" in p.args["query"]


def test_query_defaults_to_subscriber_when_unsure():
    p = parse("!query SELECT TOP 5 * FROM Subscriber")
    assert p.args["task_type"] == "sql.query_subscriber"


def test_whitespace_tolerant():
    p = parse("  !report   today  ")
    assert p.command == "report"
    assert p.subcommand == "today"


def test_non_command_ignored():
    with pytest.raises(NotACommand):
        parse("Hello")


def test_unknown_command_rejected():
    with pytest.raises(ParseError):
        parse("!frobnicate")


def test_empty_sql_rejected():
    with pytest.raises(ParseError):
        parse("!query")


def test_empty_sql_whitespace_rejected():
    with pytest.raises(ParseError):
        parse("!query    ")


def test_sql_with_stacked_statements_rejected():
    with pytest.raises(ParseError):
        parse("!query SELECT 1; DELETE FROM x")


def test_sql_with_trailing_semicolon_allowed():
    # Trailing ';' is a common habit; the agent's gate still has final say.
    p = parse("!query SELECT TOP 5 * FROM TRAFFIC.dbo.Incoming;")
    assert p.command == "query"


def test_parsed_command_is_tuple_unpackable():
    p = parse("!status")
    cmd, sub, args = p
    assert (cmd, sub, args) == ("status", None, {})


def test_empty_command_body_rejected():
    with pytest.raises(ParseError):
        parse("!")


def test_empty_input_not_a_command():
    with pytest.raises(NotACommand):
        parse("")


def test_case_insensitive_command():
    p = parse("!HELP")
    assert p.command == "help"


def test_case_insensitive_subcommand():
    p = parse("!REPORT TODAY")
    assert p.subcommand == "today"
