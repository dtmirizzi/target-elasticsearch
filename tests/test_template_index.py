"""Tests for ElasticSink._template_index — date helpers, sanitization, mutable-default safety."""

import datetime

import time_machine
from tests.conftest import make_sink


class TestTemplateIndexBasic:
    def test_default_format_uses_today(self):
        with time_machine.travel(datetime.datetime(2024, 6, 15, 12, 0)):
            sink, _ = make_sink()
            assert sink._template_index() == "ecs-test-stream-20240615"

    def test_stream_name_is_sanitized_and_lowercased(self):
        sink, _ = make_sink(index_format="STREAM-{{ stream_name }}")
        # underscores become dashes, anything outside [a-z0-9-] is dropped
        assert sink._template_index() == "stream-test-stream"

    def test_invalid_chars_stripped(self):
        sink, _ = make_sink(index_format="my!index@{{ stream_name }}*name")
        assert sink._template_index() == "myindextest-streamname"


class TestTemplateIndexHelpers:
    def test_to_daily_helper(self):
        sink, _ = make_sink(index_format="logs-{{ to_daily(ts) }}")
        assert sink._template_index({"ts": "2023-01-02T03:04:05Z"}) == "logs-20230102"

    def test_to_monthly_helper(self):
        sink, _ = make_sink(index_format="logs-{{ to_monthly(ts) }}")
        assert sink._template_index({"ts": "2023-01-02T03:04:05Z"}) == "logs-202301"

    def test_to_yearly_helper(self):
        sink, _ = make_sink(index_format="logs-{{ to_yearly(ts) }}")
        assert sink._template_index({"ts": "2023-01-02T03:04:05Z"}) == "logs-2023"

    def test_current_timestamp_daily(self):
        with time_machine.travel(datetime.datetime(2030, 12, 31, 12, 0)):
            sink, _ = make_sink(index_format="logs-{{ current_timestamp_daily }}")
            assert sink._template_index() == "logs-20301231"

    def test_current_timestamp_monthly(self):
        with time_machine.travel(datetime.datetime(2030, 12, 31, 12, 0)):
            sink, _ = make_sink(index_format="logs-{{ current_timestamp_monthly }}")
            assert sink._template_index() == "logs-203012"

    def test_current_timestamp_yearly(self):
        with time_machine.travel(datetime.datetime(2030, 12, 31, 12, 0)):
            sink, _ = make_sink(index_format="logs-{{ current_timestamp_yearly }}")
            assert sink._template_index() == "logs-2030"


class TestTemplateIndexSchemaPassthrough:
    def test_extra_schema_keys_used_in_template(self):
        sink, _ = make_sink(index_format="ecs-{{ stream_name }}-{{ region }}")
        assert sink._template_index({"region": "us-east-1"}) == "ecs-test-stream-us-east-1"

    def test_missing_schemas_argument_does_not_leak_between_calls(self):
        """Regression: previous mutable-default-arg implementation could leak state."""
        sink, _ = make_sink(index_format="ecs-{{ stream_name }}-{{ x | default('none') }}")
        first = sink._template_index({"x": "foo"})
        # Second call without schemas must not see "foo" from the first call
        second = sink._template_index()
        assert first == "ecs-test-stream-foo"
        assert second == "ecs-test-stream-none"
