"""Shared test fixtures and helpers for target-elasticsearch unit tests."""

from unittest.mock import MagicMock, patch

from target_elasticsearch.sinks import ElasticSink


def make_sink(
    metadata_fields=None,
    index_schema_fields=None,
    index_mappings=None,
    index_format="ecs-{{ stream_name }}-{{ current_timestamp_daily}}",
    extra_config=None,
):
    """Build an ElasticSink with the elasticsearch client mocked out.

    Returns a ``(sink, mock_client)`` tuple so tests can both interact with the
    sink under test and assert on the underlying client calls.
    """
    config = {
        "scheme": "http",
        "host": "localhost",
        "port": 9200,
        "request_timeout": 10,
        "retry_on_timeout": True,
        "verify_certs": True,
        "index_format": index_format,
        "metadata_fields": {"test_stream": metadata_fields} if metadata_fields is not None else {},
        "index_schema_fields": ({"test_stream": index_schema_fields} if index_schema_fields is not None else {}),
        "index_mappings": {"test_stream": index_mappings} if index_mappings is not None else {},
    }
    if extra_config:
        config.update(extra_config)

    mock_target = MagicMock()
    mock_target.config = config
    mock_target._get_package_version.return_value = "0.0.0-test"

    schema = {"properties": {"id": {"type": "string"}, "name": {"type": "string"}}}

    mock_client = MagicMock()
    with patch.object(ElasticSink, "_authenticated_client", return_value=mock_client):
        sink = ElasticSink(
            target=mock_target,
            stream_name="test_stream",
            schema=schema,
            key_properties=None,
        )
    return sink, mock_client
