from typing import Any, Dict

import pytest

from aiodynamo.expressions import (
    AndCondition,
    Comparison,
    Condition,
    F,
    HashKey,
    OrCondition,
    Parameters,
    ProjectionExpression,
    UpdateExpression,
)


@pytest.mark.parametrize(
    "pe,expression,names",
    [
        (F("foo") & F("bar"), "#n0,#n1", {"#n0": "foo", "#n1": "bar"}),
        (F("foo", 0, "bar") & F("bar"), "#n0[0].#n1,#n1", {"#n0": "foo", "#n1": "bar"}),
        (
            F("foo", "12", "bar") & F("bar"),
            "#n0.#n1.#n2,#n2",
            {"#n0": "foo", "#n1": "12", "#n2": "bar"},
        ),
    ],
)
def test_project(
    pe: ProjectionExpression, expression: str, names: Dict[str, str]
) -> None:
    params = Parameters()
    assert pe.encode(params) == expression
    payload = params.to_request_payload()
    assert payload["ExpressionAttributeNames"] == names
    assert "ExpressionAttributeValues" not in payload


@pytest.mark.parametrize(
    "exp,ue,ean,eav",
    [
        (
            F("d").set({"k": "v"}),
            "SET #n0 = :v0",
            {"#n0": "d"},
            {":v0": {"M": {"k": {"S": "v"}}}},
        ),
        (
            F("iz").set(0) & F("bf").set(False) & F("io").set(1) & F("bt").set(True),
            "SET #n0 = :v0, #n1 = :v1, #n2 = :v2, #n3 = :v3",
            {"#n0": "iz", "#n1": "bf", "#n2": "io", "#n3": "bt"},
            {
                ":v0": {"N": "0"},
                ":v1": {"BOOL": False},
                ":v2": {"N": "1"},
                ":v3": {"BOOL": True},
            },
        ),
    ],
)
def test_update_expression(
    exp: UpdateExpression, ue: str, ean: Dict[str, str], eav: Dict[str, Dict[str, Any]]
) -> None:
    params = Parameters()
    assert exp.encode(params) == ue
    payload = params.to_request_payload()
    assert payload["ExpressionAttributeNames"] == ean
    assert payload["ExpressionAttributeValues"] == eav


@pytest.mark.parametrize(
    "hash_key,encoded,ean,eav",
    [
        (HashKey("h", "v"), "#n0 = :v0", {"#n0": "h"}, {":v0": {"S": "v"}}),
        (
            HashKey("hash_key", "value"),
            "#n0 = :v0",
            {"#n0": "hash_key"},
            {":v0": {"S": "value"}},
        ),
    ],
)
def test_hash_key_encoding(
    hash_key: HashKey, encoded: str, ean: Dict[str, str], eav: Dict[str, Dict[str, str]]
) -> None:
    params = Parameters()
    assert hash_key.encode(params) == encoded
    payload = params.to_request_payload()
    assert payload["ExpressionAttributeNames"] == ean
    assert payload["ExpressionAttributeValues"] == eav


@pytest.mark.parametrize(
    "lhs,rhs,eq",
    [
        (F("foo"), F("foo"), True),
        (F("foo"), F("bar"), False),
        (F("foo"), True, False),
        (F("foo", 1, "bar"), F("foo", 1, "bar"), True),
        (F("foo", 1, "bar"), F("foo", 2, "bar"), False),
        (F("foo", 1, "bar"), F("foo", 1, "baz"), False),
    ],
)
def test_f_eq(lhs: F, rhs: F, eq: bool) -> None:
    assert (lhs == rhs) is eq


@pytest.mark.parametrize(
    "f,r", [(F("foo"), "F(foo)"), (F("foo", 1, "bar"), "F(foo.1.bar)")]
)
def test_f_repr(f: F, r: str) -> None:
    assert repr(f) == r


@pytest.mark.parametrize(
    "expr,expected",
    [
        (F("a").equals(True) & F("b").gt(1), "(a = True AND b > 1)"),
        (F("a", 1).begins_with("foo"), "begins_with(a[1], 'foo')"),
        (
            F("a").equals("a") & F("b").equals("b") & F("c").equals("c"),
            "(a = 'a' AND b = 'b' AND c = 'c')",
        ),
    ],
)
def test_condition_debug(expr: Condition, expected: str) -> None:
    assert expr.debug(int) == expected


@pytest.mark.parametrize(
    "expr,expected",
    [
        (
            F("a").set(1) & F("b").add(2) & F("c").remove() & F("d").delete({"e"}),
            "SET a = 1 REMOVE c ADD b 2 DELETE d {'e'}",
        ),
        (F("foo", "bar", 1).set("test"), "SET foo.bar[1] = 'test'"),
    ],
)
def test_update_expression_debug(expr: UpdateExpression, expected: str) -> None:
    assert expr.debug(int) == expected


@pytest.mark.parametrize(
    "expr,expected",
    [
        (
            F("a").equals("a") & F("b").equals("b"),
            AndCondition((Comparison(F("a"), "=", "a"), Comparison(F("b"), "=", "b"))),
        ),
        (
            (F("a").equals("a") & F("b").equals("b")) & F("c").equals("c"),
            AndCondition(
                (
                    Comparison(F("a"), "=", "a"),
                    Comparison(F("b"), "=", "b"),
                    Comparison(F("c"), "=", "c"),
                )
            ),
        ),
        (
            (F("a").equals("a") & F("b").equals("b"))
            & (F("c").equals("c") & F("d").equals("d")),
            AndCondition(
                (
                    Comparison(F("a"), "=", "a"),
                    Comparison(F("b"), "=", "b"),
                    Comparison(F("c"), "=", "c"),
                    Comparison(F("d"), "=", "d"),
                )
            ),
        ),
        (
            F("a").equals("a") | F("b").equals("b"),
            OrCondition((Comparison(F("a"), "=", "a"), Comparison(F("b"), "=", "b"))),
        ),
        (
            (F("a").equals("a") | F("b").equals("b")) | F("c").equals("c"),
            OrCondition(
                (
                    Comparison(F("a"), "=", "a"),
                    Comparison(F("b"), "=", "b"),
                    Comparison(F("c"), "=", "c"),
                )
            ),
        ),
        (
            (F("a").equals("a") | F("b").equals("b"))
            | (F("c").equals("c") | F("d").equals("d")),
            OrCondition(
                (
                    Comparison(F("a"), "=", "a"),
                    Comparison(F("b"), "=", "b"),
                    Comparison(F("c"), "=", "c"),
                    Comparison(F("d"), "=", "d"),
                )
            ),
        ),
        (
            (F("a").equals("a") | F("b").equals("b"))
            & (F("c").equals("c") | F("d").equals("d")),
            AndCondition(
                (
                    OrCondition(
                        (Comparison(F("a"), "=", "a"), Comparison(F("b"), "=", "b"))
                    ),
                    OrCondition(
                        (Comparison(F("c"), "=", "c"), Comparison(F("d"), "=", "d"))
                    ),
                )
            ),
        ),
    ],
)
def test_condition_flattening(expr: Condition, expected: Condition) -> None:
    assert expr == expected
