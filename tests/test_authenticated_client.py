"""Tests for ElasticSink._authenticated_client — covers each auth permutation and SSL handling."""

from unittest.mock import MagicMock, patch

from target_elasticsearch.sinks import ElasticSink


def _build_sink_with_config(extra_config):
    """Construct a sink so __init__ runs through _authenticated_client.

    Returns the kwargs that the patched ``elasticsearch.Elasticsearch`` was called with.
    """
    config = {
        "scheme": "http",
        "host": "es.example.com",
        "port": 9200,
        "request_timeout": 10,
        "retry_on_timeout": True,
        "verify_certs": True,
        "index_format": "ecs-{{ stream_name }}-{{ current_timestamp_daily}}",
        "metadata_fields": {},
        "index_schema_fields": {},
        "index_mappings": {},
    }
    config.update(extra_config)

    mock_target = MagicMock()
    mock_target.config = config
    mock_target._get_package_version.return_value = "0.0.0-test"

    schema = {"properties": {"id": {"type": "string"}}}

    with patch("target_elasticsearch.sinks.elasticsearch.Elasticsearch") as mock_es:
        ElasticSink(
            target=mock_target,
            stream_name="test_stream",
            schema=schema,
            key_properties=None,
        )
        return mock_es.call_args.kwargs


class TestSchemeAndHost:
    def test_default_http(self):
        kwargs = _build_sink_with_config({})
        assert kwargs["hosts"] == ["http://es.example.com:9200"]
        assert kwargs["request_timeout"] == 10
        assert kwargs["retry_on_timeout"] is True
        assert kwargs["verify_certs"] is True

    def test_https_scheme(self):
        kwargs = _build_sink_with_config({"scheme": "https"})
        assert kwargs["hosts"] == ["https://es.example.com:9200"]

    def test_ssl_ca_file_forces_https_and_sets_ca_certs(self):
        kwargs = _build_sink_with_config({"scheme": "http", "ssl_ca_file": "/etc/ca.pem"})
        assert kwargs["hosts"] == ["https://es.example.com:9200"]
        assert kwargs["ca_certs"] == "/etc/ca.pem"

    def test_verify_certs_false(self):
        kwargs = _build_sink_with_config({"verify_certs": False})
        assert kwargs["verify_certs"] is False


class TestAuthPermutations:
    def test_basic_auth(self):
        kwargs = _build_sink_with_config({"username": "elastic", "password": "secret"})
        assert kwargs["basic_auth"] == ("elastic", "secret")
        assert "api_key" not in kwargs
        assert "bearer_auth" not in kwargs

    def test_api_key_pair(self):
        kwargs = _build_sink_with_config({"api_key_id": "abc", "api_key": "xyz"})
        assert kwargs["api_key"] == ("abc", "xyz")
        assert "basic_auth" not in kwargs

    def test_encoded_api_key(self):
        kwargs = _build_sink_with_config({"encoded_api_key": "ZW5jb2RlZA=="})
        assert kwargs["api_key"] == "ZW5jb2RlZA=="

    def test_bearer_token(self):
        kwargs = _build_sink_with_config({"bearer_token": "tok"})
        assert kwargs["bearer_auth"] == "tok"

    def test_basic_auth_takes_precedence_over_api_key(self):
        kwargs = _build_sink_with_config(
            {
                "username": "elastic",
                "password": "secret",
                "api_key_id": "abc",
                "api_key": "xyz",
            }
        )
        assert kwargs["basic_auth"] == ("elastic", "secret")
        assert "api_key" not in kwargs

    def test_no_auth_does_not_set_credentials(self):
        kwargs = _build_sink_with_config({})
        assert "basic_auth" not in kwargs
        assert "api_key" not in kwargs
        assert "bearer_auth" not in kwargs


class TestUserAgentHeader:
    def test_user_agent_header_is_set(self):
        kwargs = _build_sink_with_config({})
        assert "headers" in kwargs
        assert "user-agent" in kwargs["headers"]
        assert kwargs["headers"]["user-agent"].startswith("meltano-loader-elasticsearch/")
