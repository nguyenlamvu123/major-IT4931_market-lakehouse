"""Reshaping helpers used by the dashboard endpoints."""

from __future__ import annotations

from serving import views


def test_normalized_compare_rebases_each_symbol_to_100() -> None:
    rows = views.normalized_compare(
        {
            "SPY": [{"day": "2026-11-03", "close": 606.0}, {"day": "2026-11-02", "close": 600.0}],
            "GLD": [{"day": "2026-11-03", "close": 250.0}],
        }
    )
    assert rows == [
        {"day": "2026-11-02", "SPY": 100.0, "GLD": None},
        {"day": "2026-11-03", "SPY": 101.0, "GLD": 100.0},
    ]


def test_correlation_matrix_is_symmetric_with_unit_diagonal() -> None:
    matrix = views.correlation_matrix(
        [
            {"base_symbol": "GLD", "quote_symbol": "US10Y", "correlation": -0.4},
            {"base_symbol": "GLD", "quote_symbol": "SPY", "correlation": 0.1},
        ]
    )
    by_symbol = {row["symbol"]: row for row in matrix}
    assert list(by_symbol) == ["GLD", "SPY", "US10Y"]
    assert by_symbol["US10Y"]["GLD"] == by_symbol["GLD"]["US10Y"] == -0.4
    assert by_symbol["SPY"]["SPY"] == 1.0
    assert by_symbol["SPY"]["US10Y"] is None


def test_curve_points_skip_missing_tenors() -> None:
    points = views.curve_points({"day": "2026-11-09", "y_2y": 4.0, "y_10y": 4.2, "y_30y": None})
    assert [(point["tenor"], point["years"], point["yield"]) for point in points] == [
        ("2Y", 2.0, 4.0),
        ("10Y", 10.0, 4.2),
    ]
    assert views.curve_points(None) == []
