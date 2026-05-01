"""Tests for ElasticSink.create_index — covers exists/missing/mapping update/error branches."""

from unittest.mock import MagicMock

import elasticsearch
import pytest
from tests.conftest import make_sink


class TestCreateIndexNew:
    def test_creates_when_missing(self):
        sink, client = make_sink()
        client.indices.exists.return_value = False

        sink.create_index("my-index")

        client.indices.create.assert_called_once_with(index="my-index", mappings={"properties": {}})

    def test_creates_with_configured_mappings(self):
        mappings = {"email": {"type": "keyword"}}
        sink, client = make_sink(index_mappings=mappings)
        client.indices.exists.return_value = False

        sink.create_index("my-index")

        client.indices.create.assert_called_once_with(index="my-index", mappings={"properties": mappings})


class TestCreateIndexExisting:
    def test_existing_with_no_mappings_skips(self):
        sink, client = make_sink()
        client.indices.exists.return_value = True

        sink.create_index("my-index")

        client.indices.create.assert_not_called()
        client.indices.put_mapping.assert_not_called()

    def test_existing_mapping_already_matches_does_not_put(self):
        sink, client = make_sink(index_mappings={"email": {"type": "keyword"}})
        client.indices.exists.return_value = True
        client.indices.get_field_mapping.return_value = {
            "my-index": {"mappings": {"email": {"mapping": {"email": {"type": "keyword"}}}}}
        }

        sink.create_index("my-index")

        client.indices.put_mapping.assert_not_called()

    def test_existing_mapping_mismatch_calls_put_mapping(self):
        sink, client = make_sink(index_mappings={"email": {"type": "keyword"}})
        client.indices.exists.return_value = True
        client.indices.get_field_mapping.return_value = {
            "my-index": {"mappings": {"email": {"mapping": {"email": {"type": "text"}}}}}
        }

        sink.create_index("my-index")

        client.indices.put_mapping.assert_called_once_with(
            index="my-index", body={"properties": {"email": {"type": "keyword"}}}
        )


class TestCreateIndexBadRequest:
    def _make_bad_request_error(self, message):
        # BadRequestError.__str__ touches meta.status, so we provide a minimal
        # meta-like object to keep logging-stringification from blowing up.
        meta = MagicMock()
        meta.status = 400
        return elasticsearch.exceptions.BadRequestError(message=message, meta=meta, body=None)

    def test_illegal_argument_exception_is_swallowed_with_warning(self):
        sink, client = make_sink(index_mappings={"email": {"type": "keyword"}})
        client.indices.exists.return_value = True
        client.indices.get_field_mapping.return_value = {
            "my-index": {"mappings": {"email": {"mapping": {"email": {"type": "text"}}}}}
        }
        client.indices.put_mapping.side_effect = self._make_bad_request_error("illegal_argument_exception")

        # Should not raise — the illegal_argument_exception branch warns and continues.
        sink.create_index("my-index")

        sink.logger.warning.assert_called_once()
        warned = sink.logger.warning.call_args.args[0]
        assert "Failed to update mapping" in warned

    def test_other_bad_request_is_reraised(self):
        sink, client = make_sink(index_mappings={"email": {"type": "keyword"}})
        client.indices.exists.return_value = True
        client.indices.get_field_mapping.return_value = {
            "my-index": {"mappings": {"email": {"mapping": {"email": {"type": "text"}}}}}
        }
        client.indices.put_mapping.side_effect = self._make_bad_request_error("some_other_error")

        with pytest.raises(elasticsearch.exceptions.BadRequestError):
            sink.create_index("my-index")
