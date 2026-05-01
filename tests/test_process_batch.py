"""Tests for ElasticSink.process_batch — happy path and error propagation."""

from unittest.mock import patch

import elasticsearch.helpers
import pytest
from tests.conftest import make_sink


class TestProcessBatchHappyPath:
    def test_calls_bulk_with_built_records(self):
        sink, client = make_sink()
        sink.index_name = "ecs-test-stream-20240101"
        # The sink doesn't need to create the index when index_schema_fields is empty
        # (that's done in setup()), so distinct_indices stays empty here.
        records = [{"id": "1"}, {"id": "2"}]

        with patch("target_elasticsearch.sinks.bulk") as mock_bulk:
            sink.process_batch({"records": records})

        mock_bulk.assert_called_once()
        passed_client, passed_actions = mock_bulk.call_args.args
        assert passed_client is client
        actions = list(passed_actions)
        assert len(actions) == 2
        assert all(a["_op_type"] == "index" for a in actions)
        assert all(a["_index"] == "ecs-test-stream-20240101" for a in actions)

    def test_creates_distinct_indices_before_bulk(self):
        sink, client = make_sink(
            index_schema_fields={"region": "region"},
            index_format="ecs-{{ stream_name }}-{{ region }}",
        )
        client.indices.exists.return_value = False
        records = [
            {"id": "1", "region": "us"},
            {"id": "2", "region": "eu"},
            {"id": "3", "region": "us"},
        ]

        with patch("target_elasticsearch.sinks.bulk"):
            sink.process_batch({"records": records})

        # Two distinct indices, each created exactly once
        created = {call.kwargs["index"] for call in client.indices.create.call_args_list}
        assert created == {"ecs-test-stream-us", "ecs-test-stream-eu"}


class TestProcessBatchErrorPropagation:
    def test_bulk_index_error_is_reraised(self):
        sink, _ = make_sink()
        sink.index_name = "ecs-test-stream-20240101"

        bulk_error = elasticsearch.helpers.BulkIndexError(
            "1 document(s) failed to index.",
            [{"index": {"_index": "x", "error": {"type": "mapper_parsing_exception"}}}],
        )

        with patch("target_elasticsearch.sinks.bulk", side_effect=bulk_error):
            with pytest.raises(elasticsearch.helpers.BulkIndexError):
                sink.process_batch({"records": [{"id": "1"}]})

    def test_bulk_index_error_is_logged_before_raise(self):
        sink, _ = make_sink()
        sink.index_name = "ecs-test-stream-20240101"

        errors = [{"index": {"_index": "x", "error": "boom"}}]
        bulk_error = elasticsearch.helpers.BulkIndexError("fail", errors)

        with patch("target_elasticsearch.sinks.bulk", side_effect=bulk_error):
            with pytest.raises(elasticsearch.helpers.BulkIndexError):
                sink.process_batch({"records": [{"id": "1"}]})

        sink.logger.error.assert_called_once_with(errors)
