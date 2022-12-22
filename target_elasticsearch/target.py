from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_elasticsearch import sinks
from target_elasticsearch.common import (
    INDEX_FORMAT,
    SCHEME,
    HOST,
    PORT,
    USERNAME,
    PASSWORD,
    BEARER_TOKEN,
    API_KEY_ID,
    API_KEY,
    SSL_CA_FILE,
    SCHEMA_MAPPING,
)


class TargetElasticsearch(Target):
    """Sample target for parquet."""

    name = "target-elasticsearch"
    config_jsonschema = th.PropertiesList(
        th.Property(
            SCHEME,
            th.StringType,
            description="http scheme used for connecting to elasticsearch",
            default="http",
            required=True,
        ),
        th.Property(
            HOST,
            th.StringType,
            description="host used to connect to elasticsearch",
            default="localhost",
            required=True,
        ),
        th.Property(
            PORT,
            th.NumberType,
            description="port use to connect to elasticsearch",
            default=9200,
            required=True,
        ),
        th.Property(
            USERNAME,
            th.StringType,
            description="basic auth username",
            default=None,
        ),
        th.Property(
            PASSWORD,
            th.StringType,
            description="basic auth password",
            default=None,
        ),
        th.Property(
            BEARER_TOKEN,
            th.StringType,
            description="bearer token for bearer authorization",
            default=None,
        ),
        th.Property(
            API_KEY_ID,
            th.StringType,
            description="api key id for auth key authorization",
            default=None,
        ),
        th.Property(
            API_KEY,
            th.StringType,
            description="api key for auth key authorization",
            default=None,
        ),
        th.Property(
            SSL_CA_FILE,
            th.StringType,
            description="location of the the SSL certificate for cert verification ie. `/some/path`",
            default=None,
        ),
        th.Property(
            INDEX_FORMAT,
            th.StringType,
            description="can be used to handle custom index formatting such as specifying `-latest` index. available "
            "options: Monthly {.current_timestamp_monthly} or Yearly {.current_timestamp_yearly} ",
            default="ecs-{{ stream_name }}-{{ current_timestamp_daily}}",
        ),
        th.Property(
            SCHEMA_MAPPING,
            th.ObjectType(),
            description="this schema_mapping allows you to specify specific record values from the steam to be used as "
            "ECS schema types such as (_id, @timestamp)",
            default=None,
        ),
    ).to_dict()
    default_sink_class = sinks.ElasticSink

    @property
    def state(self) -> dict:
        return {}
