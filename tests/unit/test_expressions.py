import pytest
from aiodynamo.expressions import F, Parameters


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
def test_project(pe, expression, names):
    params = Parameters()
    assert pe.encode(params) == expression
    assert params.get_expression_names() == names
    assert params.get_expression_values() == {}


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
def test_update_expression(exp, ue, ean, eav):
    params = Parameters()
    assert exp.encode(params) == ue
    assert params.get_expression_names() == ean
    assert params.get_expression_values() == eav
