"""Tests for EventFilter."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.core.filters import EventFilter

# =========================================================================
# Exact match
# =========================================================================


class TestExactMatch:
    def test_simple_equality(self):
        event = {"symbol": "AAPL", "side": "buy"}
        assert EventFilter.matches(event, {"symbol": "AAPL"}) is True

    def test_simple_inequality(self):
        event = {"symbol": "AAPL", "side": "buy"}
        assert EventFilter.matches(event, {"symbol": "GOOG"}) is False

    def test_multiple_criteria_all_match(self):
        event = {"symbol": "AAPL", "side": "buy", "qty": 100}
        assert EventFilter.matches(event, {"symbol": "AAPL", "side": "buy"}) is True

    def test_multiple_criteria_one_fails(self):
        event = {"symbol": "AAPL", "side": "buy", "qty": 100}
        assert EventFilter.matches(event, {"symbol": "AAPL", "side": "sell"}) is False

    def test_empty_criteria_matches_everything(self):
        event = {"symbol": "AAPL", "side": "buy"}
        assert EventFilter.matches(event, {}) is True

    def test_missing_field_does_not_match(self):
        event = {"symbol": "AAPL"}
        assert EventFilter.matches(event, {"side": "buy"}) is False

    def test_none_value_matches_none_criteria(self):
        event = {"symbol": None}
        assert EventFilter.matches(event, {"symbol": None}) is True

    def test_numeric_equality(self):
        event = {"qty": 100}
        assert EventFilter.matches(event, {"qty": 100}) is True
        assert EventFilter.matches(event, {"qty": 200}) is False

    def test_boolean_equality(self):
        event = {"active": True}
        assert EventFilter.matches(event, {"active": True}) is True
        assert EventFilter.matches(event, {"active": False}) is False


# =========================================================================
# $eq equivalent (direct value comparison -- no operator dict)
# =========================================================================


class TestNe:
    def test_ne_match(self):
        event = {"status": "filled"}
        assert EventFilter.matches(event, {"status": {"$ne": "cancelled"}}) is True

    def test_ne_no_match(self):
        event = {"status": "cancelled"}
        assert EventFilter.matches(event, {"status": {"$ne": "cancelled"}}) is False

    def test_ne_with_none(self):
        event = {"status": None}
        assert EventFilter.matches(event, {"status": {"$ne": "cancelled"}}) is True


# =========================================================================
# $gt, $lt, $gte, $lte
# =========================================================================


class TestComparison:
    def test_gt_match(self):
        event = {"price": 150.0}
        assert EventFilter.matches(event, {"price": {"$gt": 100.0}}) is True

    def test_gt_no_match(self):
        event = {"price": 50.0}
        assert EventFilter.matches(event, {"price": {"$gt": 100.0}}) is False

    def test_gt_boundary(self):
        event = {"price": 100.0}
        assert EventFilter.matches(event, {"price": {"$gt": 100.0}}) is False

    def test_lt_match(self):
        event = {"price": 50.0}
        assert EventFilter.matches(event, {"price": {"$lt": 100.0}}) is True

    def test_lt_no_match(self):
        event = {"price": 150.0}
        assert EventFilter.matches(event, {"price": {"$lt": 100.0}}) is False

    def test_lt_boundary(self):
        event = {"price": 100.0}
        assert EventFilter.matches(event, {"price": {"$lt": 100.0}}) is False

    def test_gte_match(self):
        event = {"qty": 100}
        assert EventFilter.matches(event, {"qty": {"$gte": 100}}) is True

    def test_gte_below(self):
        event = {"qty": 99}
        assert EventFilter.matches(event, {"qty": {"$gte": 100}}) is False

    def test_lte_match(self):
        event = {"qty": 100}
        assert EventFilter.matches(event, {"qty": {"$lte": 100}}) is True

    def test_lte_above(self):
        event = {"qty": 101}
        assert EventFilter.matches(event, {"qty": {"$lte": 100}}) is False

    def test_combined_range(self):
        """Multiple comparison operators on different fields."""
        event = {"price": 150.0, "qty": 50}
        criteria = {
            "price": {"$gt": 100.0},
            "qty": {"$lte": 100},
        }
        assert EventFilter.matches(event, criteria) is True


# =========================================================================
# $in, $nin (via $in negation)
# =========================================================================


class TestIn:
    def test_in_match(self):
        event = {"status": "filled"}
        assert EventFilter.matches(event, {"status": {"$in": ["filled", "partial"]}}) is True

    def test_in_no_match(self):
        event = {"status": "cancelled"}
        assert EventFilter.matches(event, {"status": {"$in": ["filled", "partial"]}}) is False

    def test_in_with_numbers(self):
        event = {"qty": 100}
        assert EventFilter.matches(event, {"qty": {"$in": [50, 100, 200]}}) is True

    def test_in_empty_list(self):
        event = {"status": "filled"}
        assert EventFilter.matches(event, {"status": {"$in": []}}) is False

    def test_in_with_none_value(self):
        event = {"status": None}
        assert EventFilter.matches(event, {"status": {"$in": [None, "filled"]}}) is True


# =========================================================================
# $exists
# =========================================================================


class TestExists:
    def test_exists_true_field_present(self):
        event = {"symbol": "AAPL"}
        assert EventFilter.matches(event, {"symbol": {"$exists": True}}) is True

    def test_exists_true_field_missing(self):
        event = {"price": 100}
        assert EventFilter.matches(event, {"symbol": {"$exists": True}}) is False

    def test_exists_false_field_missing(self):
        event = {"price": 100}
        assert EventFilter.matches(event, {"symbol": {"$exists": False}}) is True

    def test_exists_false_field_present(self):
        event = {"symbol": "AAPL"}
        assert EventFilter.matches(event, {"symbol": {"$exists": False}}) is False

    def test_exists_none_value_is_present(self):
        """Per MongoDB semantics: a key with None value still exists."""
        event = {"symbol": None}
        assert EventFilter.matches(event, {"symbol": {"$exists": True}}) is True
        assert EventFilter.matches(event, {"symbol": {"$exists": False}}) is False


# =========================================================================
# $regex
# =========================================================================


class TestRegex:
    def test_regex_match(self):
        event = {"symbol": "AAPL"}
        assert EventFilter.matches(event, {"symbol": {"$regex": "^AA"}}) is True

    def test_regex_no_match(self):
        event = {"symbol": "GOOG"}
        assert EventFilter.matches(event, {"symbol": {"$regex": "^AA"}}) is False

    def test_regex_full_pattern(self):
        event = {"email": "user@example.com"}
        assert EventFilter.matches(event, {"email": {"$regex": r"^[\w.]+@[\w.]+\.\w+$"}}) is True

    def test_regex_numeric_coerced_to_str(self):
        event = {"code": 404}
        assert EventFilter.matches(event, {"code": {"$regex": "^4"}}) is True


# =========================================================================
# $contains, $startswith, $endswith
# =========================================================================


class TestStringOperators:
    def test_contains_match(self):
        event = {"description": "Order filled at market price"}
        assert EventFilter.matches(event, {"description": {"$contains": "filled"}}) is True

    def test_contains_no_match(self):
        event = {"description": "Order placed"}
        assert EventFilter.matches(event, {"description": {"$contains": "filled"}}) is False

    def test_startswith_match(self):
        event = {"symbol": "AAPL"}
        assert EventFilter.matches(event, {"symbol": {"$startswith": "AA"}}) is True

    def test_startswith_no_match(self):
        event = {"symbol": "GOOG"}
        assert EventFilter.matches(event, {"symbol": {"$startswith": "AA"}}) is False

    def test_endswith_match(self):
        event = {"filename": "report.pdf"}
        assert EventFilter.matches(event, {"filename": {"$endswith": ".pdf"}}) is True

    def test_endswith_no_match(self):
        event = {"filename": "report.csv"}
        assert EventFilter.matches(event, {"filename": {"$endswith": ".pdf"}}) is False


# =========================================================================
# Nested field access (dot notation)
# =========================================================================


class TestNestedFields:
    def test_simple_nested(self):
        event = {"payload": {"symbol": "AAPL", "price": 150.0}}
        assert EventFilter.matches(event, {"payload.symbol": "AAPL"}) is True

    def test_simple_nested_no_match(self):
        event = {"payload": {"symbol": "AAPL"}}
        assert EventFilter.matches(event, {"payload.symbol": "GOOG"}) is False

    def test_deep_nested(self):
        event = {
            "metadata": {
                "user": {
                    "id": "user_123",
                    "role": "admin",
                }
            }
        }
        assert EventFilter.matches(event, {"metadata.user.role": "admin"}) is True

    def test_nested_with_operator(self):
        event = {"payload": {"price": 150.0}}
        assert EventFilter.matches(event, {"payload.price": {"$gt": 100.0}}) is True

    def test_nested_missing_intermediate(self):
        event = {"data": "flat"}
        result = EventFilter.matches(event, {"data.nested": "value"})
        assert result is False

    def test_nested_non_dict_intermediate(self):
        event = {"data": 42}
        result = EventFilter.matches(event, {"data.nested": "value"})
        assert result is False

    def test_nested_with_in_operator(self):
        event = {"metadata": {"user_id": "user_123"}}
        assert (
            EventFilter.matches(event, {"metadata.user_id": {"$in": ["user_123", "user_456"]}})
            is True
        )


# =========================================================================
# Control filters
# =========================================================================


class TestControlFilters:
    def test_start_from_latest_always_matches(self):
        event = {"symbol": "AAPL"}
        criteria = {"start_from_latest": True}
        assert EventFilter.matches(event, criteria) is True

    def test_start_from_latest_with_any_event(self):
        event = {}
        criteria = {"start_from_latest": False}
        assert EventFilter.matches(event, criteria) is True


# =========================================================================
# Edge cases
# =========================================================================


class TestEdgeCases:
    def test_empty_event_empty_criteria(self):
        assert EventFilter.matches({}, {}) is True

    def test_empty_event_with_criteria(self):
        assert EventFilter.matches({}, {"key": "value"}) is False

    def test_complex_combined_filter(self):
        event = {
            "event_type": "OrderFilled",
            "symbol": "AAPL",
            "quantity": 100,
            "price": 150.25,
            "metadata": {"user_id": "user_123"},
        }
        criteria = {
            "event_type": {"$in": ["OrderFilled", "OrderPlaced"]},
            "symbol": {"$startswith": "AA"},
            "quantity": {"$gte": 50},
            "price": {"$lt": 200.0},
            "metadata.user_id": "user_123",
        }
        assert EventFilter.matches(event, criteria) is True
