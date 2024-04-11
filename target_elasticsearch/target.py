from typing import Dict
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
    ENCODED_API_KEY,
    SSL_CA_FILE,
    INDEX_TEMPLATE_FIELDS,
    METADATA_FIELDS,
    REQUEST_TIMEOUT,
    RETRY_ON_TIMEOUT,
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
            ENCODED_API_KEY,
            th.StringType,
            description="Encoded api key for auth key authorization",
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
            description="""Index Format is used to handle custom index formatting such as specifying `-latest` index.
    ie. the default index string defined as:
    `ecs-{{ stream_name }}-{{ current_timestamp_daily}}` -> `ecs-animals-2022-12-25` where the stream name was animals

    Default options:
    Daily `{{ current_timestamp_daily }}` -> 2022-12-25,
    Monthly `{{ current_timestamp_monthly }}`->  2022-12,
    Yearly `{{ current_timestamp_yearly }}` -> 2022.
    You can also use fields mapped in `index_schema_fields` such as `{{ x }}` or `{{ timestamp }}`.

    There are also helper functions such as:
    to daily `{{ to_daily(timestamp) }}`,
    to monthly `{{ to_monthly(timestamp) }}`,
    to yearly `{{ to_yearly(timestamp) }}`
            """,
            default="ecs-{{ stream_name }}-{{ current_timestamp_daily}}",
        ),
        th.Property(
            INDEX_TEMPLATE_FIELDS,
            th.ObjectType(),
            description="""Index Schema Fields allows you to specify specific record values via jsonpath
    from the stream to be used in index formulation.
    ie. if the stream record looks like `{"id": "1", "created_at": "12-13-202000:01:43Z"}`
    and we want to index the record via create time.
    we could specify a mapping like `index_timestamp: created_at`
    in the `index_format` we could use a template like `ecs-animals-{{ to_daily(index_timestamp) }}`
    this would put this record onto the index  `ecs-animals-2020-12-13`""",
            default=None,
        ),
        th.Property(
            METADATA_FIELDS,
            th.ObjectType(),
            description="""Metadata Fields can be used to pull out specific fields via jsonpath to be
    used on for [ecs metadata patters](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html)
    This would best be used for data that has a primary key.
    ie. `{"guid": 102, "foo": "bar"}`
    then create a mapping of `_id: guid""",
            default=None,
        ),
        th.Property(
            REQUEST_TIMEOUT,
            th.NumberType,
            description="request timeout in seconds",
            default=10,
        ),
        th.Property(
            RETRY_ON_TIMEOUT,
            th.BooleanType,
            description="retry failed requests on timeout",
            default=True,
        ),
    ).to_dict()
    default_sink_class = sinks.ElasticSink

    @property
    def state(self) -> Dict:
        return {}
