import pytest

from aiodynamo.models import F


@pytest.mark.parametrize(
    "pe,expression,names",
    [
        (F("foo") & F("bar"), "#N0,#N1", {"#N0": "foo", "#N1": "bar"}),
        (F("foo", 0, "bar") & F("bar"), "#N0[0].#N1,#N1", {"#N0": "foo", "#N1": "bar"}),
        (
            F("foo", "12", "bar") & F("bar"),
            "#N0.#N1.#N2,#N2",
            {"#N0": "foo", "#N1": "12", "#N2": "bar"},
        ),
    ],
)
def test_project(pe, expression, names):
    assert pe.encode() == (expression, names)


@pytest.mark.parametrize(
    "exp,ue,ean,eav",
    [(F("d").set({"k": "v"}), "SET #N0 = :V0", {"#N0": "d"}, {":V0": {"k": "v"}})],
)
def test_update_expression(exp, ue, ean, eav):
    assert exp.encode() == (ue, ean, eav)
