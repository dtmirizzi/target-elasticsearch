"""Tests to validate the JSONPath expression caching optimization from PR #23.

PR #23 claims that jsonpath_ng.parse() is called on every record for each key in
metadata_fields and index_schema_fields, and that caching compiled expressions
in __init__ avoids this overhead. These tests verify:

1. _build_fields calls jsonpath_ng.parse() per key per record (the problem)
2. Pre-compiled expressions produce identical results to on-the-fly parsing
3. Caching eliminates redundant parse calls when compiled expressions are provided
"""

from unittest.mock import MagicMock, patch

import jsonpath_ng
import pytest

from target_elasticsearch.sinks import ElasticSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sink(metadata_fields=None, index_schema_fields=None):
    """Create an ElasticSink with mocked target/client so no ES connection is needed."""
    config = {
        "scheme": "http",
        "host": "localhost",
        "port": 9200,
        "request_timeout": 10,
        "retry_on_timeout": True,
        "index_format": "ecs-{{ stream_name }}-{{ current_timestamp_daily}}",
        "metadata_fields": {"test_stream": metadata_fields or {}},
        "index_schema_fields": {"test_stream": index_schema_fields or {}},
    }

    mock_target = MagicMock()
    mock_target.config = config
    mock_target._get_package_version.return_value = "0.0.0-test"

    schema = {"properties": {"id": {"type": "string"}, "name": {"type": "string"}, "category": {"type": "string"}}}

    with patch.object(ElasticSink, "_authenticated_client", return_value=MagicMock()):
        sink = ElasticSink(
            target=mock_target,
            stream_name="test_stream",
            schema=schema,
            key_properties=None,
        )
    return sink


# ---------------------------------------------------------------------------
# Test: _build_fields calls jsonpath_ng.parse() per key per record (baseline)
# ---------------------------------------------------------------------------


class TestJsonPathParseCalledPerRecord:
    """Verify that without caching, jsonpath_ng.parse() is called on every invocation."""

    def test_parse_called_for_each_key_each_time(self):
        """Without pre-compiled expressions, parse() is called once per key per _build_fields call."""
        sink = _make_sink()
        mapping = {"_id": "id", "category": "category"}
        record = {"id": "123", "name": "test", "category": "animals"}

        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            # Call _build_fields 5 times (simulating 5 records) without compiled expressions
            for _ in range(5):
                sink._build_fields(mapping, record)

            # parse() should be called 2 keys * 5 records = 10 times
            assert mock_parse.call_count == 10, (
                f"Expected jsonpath_ng.parse() to be called 10 times (2 keys x 5 records), "
                f"got {mock_parse.call_count}"
            )

    def test_parse_called_scales_with_records(self):
        """Parse call count scales linearly with number of records."""
        sink = _make_sink()
        mapping = {"_id": "id"}
        records = [{"id": str(i), "name": f"test_{i}"} for i in range(100)]

        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            for record in records:
                sink._build_fields(mapping, record)

            # 1 key * 100 records = 100 parse calls
            assert mock_parse.call_count == 100, (
                f"Expected 100 parse calls (1 key x 100 records), got {mock_parse.call_count}"
            )


# ---------------------------------------------------------------------------
# Test: Pre-compiled expressions produce correct results
# ---------------------------------------------------------------------------


class TestCompiledExpressionsCorrectness:
    """Verify that using pre-compiled expressions gives identical results to on-the-fly parsing."""

    def test_simple_field_extraction(self):
        """Compiled and non-compiled paths produce the same result for simple fields."""
        sink = _make_sink()
        mapping = {"_id": "id", "animal_name": "name"}
        record = {"id": "42", "name": "elephant", "category": "mammals"}

        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}

        result_without_cache = sink._build_fields(mapping, record)
        result_with_cache = sink._build_fields(mapping, record, compiled=compiled)

        assert result_without_cache == result_with_cache
        assert result_with_cache == {"_id": "42", "animal_name": "elephant"}

    def test_missing_field_handling(self):
        """Both paths handle missing fields the same way (fallback to raw value)."""
        sink = _make_sink()
        mapping = {"_id": "id", "missing_key": "nonexistent_field"}
        record = {"id": "1", "name": "test"}

        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}

        result_without_cache = sink._build_fields(mapping, record)
        result_with_cache = sink._build_fields(mapping, record, compiled=compiled)

        assert result_without_cache == result_with_cache
        # Missing fields should fall back to the raw jsonpath string as value
        assert result_with_cache["missing_key"] == "nonexistent_field"

    def test_nested_jsonpath_expression(self):
        """Compiled expressions work correctly with nested JSONPath like '$.nested.field'."""
        sink = _make_sink()
        mapping = {"deep_val": "nested.inner"}
        record = {"nested": {"inner": "deep_value"}, "top": "shallow"}

        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}

        result_without_cache = sink._build_fields(mapping, record)
        result_with_cache = sink._build_fields(mapping, record, compiled=compiled)

        assert result_without_cache == result_with_cache
        assert result_with_cache["deep_val"] == "deep_value"

    def test_multiple_records_consistent_results(self):
        """Compiled expressions produce correct results across many different records."""
        sink = _make_sink()
        mapping = {"_id": "id", "cat": "category"}

        records = [
            {"id": "1", "category": "mammals", "name": "dog"},
            {"id": "2", "category": "birds", "name": "eagle"},
            {"id": "3", "category": "reptiles", "name": "snake"},
        ]

        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}

        for record in records:
            result_without = sink._build_fields(mapping, record)
            result_with = sink._build_fields(mapping, record, compiled=compiled)
            assert result_without == result_with


# ---------------------------------------------------------------------------
# Test: Caching eliminates redundant parse calls
# ---------------------------------------------------------------------------


class TestCachingEliminatesParseCalls:
    """Verify that passing pre-compiled expressions skips jsonpath_ng.parse()."""

    def test_no_parse_calls_with_compiled_expressions(self):
        """When compiled expressions are provided, jsonpath_ng.parse() should not be called."""
        sink = _make_sink()
        mapping = {"_id": "id", "category": "category"}
        record = {"id": "123", "name": "test", "category": "animals"}

        # Pre-compile expressions (this calls parse once per key)
        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}

        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            # Call _build_fields with pre-compiled expressions
            for _ in range(100):
                sink._build_fields(mapping, record, compiled=compiled)

            # parse() should NOT be called at all since we provided compiled expressions
            assert mock_parse.call_count == 0, (
                f"Expected 0 parse calls when using compiled expressions, got {mock_parse.call_count}"
            )

    def test_parse_called_only_for_missing_compiled_keys(self):
        """If compiled dict is partial, parse() is only called for missing keys."""
        sink = _make_sink()
        mapping = {"_id": "id", "category": "category", "name": "name"}
        record = {"id": "123", "name": "test", "category": "animals"}

        # Only pre-compile 2 of 3 keys
        compiled = {"_id": jsonpath_ng.parse("id"), "category": jsonpath_ng.parse("category")}

        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            for _ in range(10):
                sink._build_fields(mapping, record, compiled=compiled)

            # parse() should only be called for the missing key "name", once per invocation
            assert mock_parse.call_count == 10, (
                f"Expected 10 parse calls (1 missing key x 10 records), got {mock_parse.call_count}"
            )


# ---------------------------------------------------------------------------
# Test: Sink __init__ pre-compiles expressions (integration)
# ---------------------------------------------------------------------------


class TestSinkInitPreCompilation:
    """Verify that ElasticSink.__init__ pre-compiles JSONPath expressions."""

    def test_compiled_metadata_fields_created(self):
        """Sink should have compiled_metadata_fields dict after init."""
        sink = _make_sink(metadata_fields={"_id": "id", "cat": "category"})

        assert hasattr(sink, "compiled_metadata_fields")
        assert set(sink.compiled_metadata_fields.keys()) == {"_id", "cat"}
        # Each value should be a compiled JSONPath expression, not a string
        for key, expr in sink.compiled_metadata_fields.items():
            assert hasattr(expr, "find"), f"compiled_metadata_fields['{key}'] is not a compiled JSONPath expression"

    def test_compiled_index_schema_fields_created(self):
        """Sink should have compiled_index_schema_fields dict after init."""
        sink = _make_sink(index_schema_fields={"timestamp": "created_at", "region": "geo.region"})

        assert hasattr(sink, "compiled_index_schema_fields")
        assert set(sink.compiled_index_schema_fields.keys()) == {"timestamp", "region"}
        for key, expr in sink.compiled_index_schema_fields.items():
            assert hasattr(expr, "find"), (
                f"compiled_index_schema_fields['{key}'] is not a compiled JSONPath expression"
            )

    def test_empty_fields_produce_empty_compiled_dicts(self):
        """When no metadata/schema fields are configured, compiled dicts should be empty."""
        sink = _make_sink(metadata_fields={}, index_schema_fields={})

        assert sink.compiled_metadata_fields == {}
        assert sink.compiled_index_schema_fields == {}

    def test_compiled_expressions_used_in_build_request_body(self):
        """build_request_body_and_distinct_indices should use pre-compiled expressions."""
        sink = _make_sink(
            metadata_fields={"_id": "id"},
            index_schema_fields={},
        )
        # Set a static index so we don't need schema fields
        sink.index_name = "test-index"

        records = [{"id": str(i), "name": f"record_{i}"} for i in range(50)]

        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            sink.build_request_body_and_distinct_indices(records)

            # With caching, parse() should NOT be called during batch processing
            assert mock_parse.call_count == 0, (
                f"Expected 0 parse calls during build_request_body (expressions should be pre-compiled), "
                f"got {mock_parse.call_count}"
            )

    def test_build_request_body_results_correct_with_caching(self):
        """Verify that cached build_request_body produces correct output."""
        sink = _make_sink(
            metadata_fields={"_id": "id"},
            index_schema_fields={},
        )
        sink.index_name = "test-index"

        records = [
            {"id": "100", "name": "alpha"},
            {"id": "200", "name": "beta"},
            {"id": "300", "name": "gamma"},
        ]

        updated_records, distinct_indices = sink.build_request_body_and_distinct_indices(records)

        assert len(updated_records) == 3
        for i, rec in enumerate(updated_records):
            assert rec["_op_type"] == "index"
            assert rec["_index"] == "test-index"
            assert rec["_id"] == records[i]["id"]
            assert rec["_source"] == records[i]


# ---------------------------------------------------------------------------
# Test: Performance characteristic (parse count comparison)
# ---------------------------------------------------------------------------


class TestPerformanceCharacteristic:
    """Compare parse() call counts between cached and uncached approaches to confirm the claim."""

    def test_uncached_vs_cached_parse_count(self):
        """Demonstrate the difference in parse() call counts: O(N*M) vs O(M)."""
        num_records = 200
        mapping = {"_id": "id", "cat": "category", "nm": "name"}
        num_keys = len(mapping)
        records = [{"id": str(i), "name": f"name_{i}", "category": f"cat_{i}"} for i in range(num_records)]

        sink = _make_sink()

        # --- Uncached: parse() called per key per record ---
        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            for record in records:
                sink._build_fields(mapping, record)
            uncached_count = mock_parse.call_count

        # --- Cached: parse() called zero times during processing ---
        compiled = {k: jsonpath_ng.parse(v) for k, v in mapping.items()}
        with patch("target_elasticsearch.sinks.jsonpath_ng.parse", wraps=jsonpath_ng.parse) as mock_parse:
            for record in records:
                sink._build_fields(mapping, record, compiled=compiled)
            cached_count = mock_parse.call_count

        assert uncached_count == num_records * num_keys, (
            f"Uncached should call parse {num_records * num_keys} times, got {uncached_count}"
        )
        assert cached_count == 0, f"Cached should call parse 0 times, got {cached_count}"

        # The savings: all parse calls eliminated
        assert uncached_count - cached_count == num_records * num_keys
