from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_elasticsearch import sinks

class TargetElastic(Target):
    """Sample target for parquet."""

    name = "target-Elastic"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "scheme",
            th.StringType,
            description="http scheme used for connecting to elasticsearch",
            default="http",
            required=True,
        ),
        th.Property(
            "host",
            th.StringType,
            description="host used to connect to elasticsearch",
            default="localhost",
            required=True,
        ),
        th.Property(
            "port",
            th.NumberType,
            description="port use to connect to elasticsearch",
            default="9200",
            required=True,
        ),
        th.Property(
            "username",
            th.StringType,
            description="basic auth username",
            default=None,
        ),
        th.Property(
            "password",
            th.StringType,
            description="basic auth password",
            default=None,
        ),
        th.Property(
            "bearer_token",
            th.StringType,
            description="bearer token for bearer authorization",
            default=None,
        ),
        th.Property(
            "api_key_id",
            th.StringType,
            description="api key id for auth key authorization",
            default=None,
        ),
        th.Property(
            "api_key",
            th.StringType,
            description="api key for auth key authorization",
            default=None,
        ),
        th.Property(
            "ssl_ca_file",
            th.StringType,
            description="location of the the SSL certificate for cert verification ie. `/some/path`",
            default=None,
        ),
        th.Property(
            "index_format",
            th.StringType,
            description="can be used to handle custom index formatting such as specifying `-latest` index. available "
                        "options: Monthly {.current_timestamp_monthly} or Yearly {.current_timestamp_yearly} ",
            default="{.stream_name}-{.current_timestamp_daily}",
        ),
        th.Property(
            "schema_mapping",
            th.ObjectType,
            description="this schema_mapping allows you to specify specific record values from the steam to be used as "
                        "ECS schema types such as (_id, @timestamp)",
            default=None,
        )
    ).to_dict()
    default_sink_class = sinks.ElasticSink

    @property
    def state(self) -> dict:
        return {}

