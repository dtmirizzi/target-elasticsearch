import elasticsearch
import jinja2

from typing import List, Dict, Optional, Union, Any

import jsonpath_ng
from elasticsearch.helpers import bulk
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink

import datetime

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
    ELASTIC_YEARLY_FORMAT,
    ELASTIC_MONTHLY_FORMAT,
    ELASTIC_DAILY_FORMAT,
)


class ElasticSink(BatchSink):
    """ElasticSink target sink class."""

    max_size = 1000  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ):
        super().__init__(target, stream_name, schema, key_properties)
        self.client = self._authenticated_client()

    def template_index(self, schemas: dict) -> str:
        """
        _index templates the input index config to be used for elasticsearch indexing
        currently it operates using current time as index.
        this may not be optimal and additional support can be added to parse @timestamp out and use it in index
        templating depending on how your elastic instance is configured.
        @param schemas:
        @return: str
        """
        today = datetime.date.today()
        arguments = {
            "stream_name": self.stream_name,
            "current_timestamp_daily": today.strftime(ELASTIC_DAILY_FORMAT),
            "current_timestamp_monthly": today.strftime(ELASTIC_MONTHLY_FORMAT),
            "current_timestamp_yearly": today.strftime(ELASTIC_YEARLY_FORMAT),
        } | schemas
        environment = jinja2.Environment()
        template = environment.from_string(self.config[INDEX_FORMAT])
        return template.render(**arguments).replace("_", "-")

    def mapped_body(self, records: List[str]) -> list[dict[Union[str, Any], Union[str, Any]]]:
        """
        mapped_body maps config schemas to record schemas
        by default the record will be placed into _source and _id and @timestamp will be left for elastic to generate
        [{
        _id: "config.schema_mapping[stream]['_id'](json path in record) || es auto generated",
        @timestamp: "config.schema_mapping[stream]['@timestamp'](json path in record) || es auto generated",
        config.schema_mapping[stream][#]: "config.schema_mapping[stream][#](json path in record)"  || None ,
        _source: {
        .record
        }
        }]
        * Not really loving this implementation *
        @param records: str
        @return: List[str]
        """
        updated_records = []
        mapping = {}
        distinct_indexes = set()
        if SCHEMA_MAPPING in self.config:
            mapping = self.config[SCHEMA_MAPPING]
        for r in records:
            schemas = {}
            if self.stream_name in mapping:
                self.logger.info(mapping[self.stream_name])
                for k, v in mapping[self.stream_name].items():
                    expression = jsonpath_ng.parse(v)
                    match = expression.find(r)
                    if len(match) == 0:
                        self.logger.warning(
                            f"schema key {k} with json path {v} could not be found for record"
                        )
                        schemas[k] = v
                    else:
                        if len(match) < 1:
                            self.logger.warning(
                                f"schema key {k} with json path {v} has multiple associated fields, not "
                                f"valid json"
                            )
                        schemas[k] = match[0].value
            index = self.template_index(schemas)
            distinct_indexes.add(index)
            updated_records.append({"_op_type": "index", "_index": index, "_source": r} | schemas)

        for index in distinct_indexes:
            try:
                self.client.indices.create(index=index)
            except elasticsearch.exceptions.RequestError as e:
                if e.error == "resource_already_exists_exception":
                    self.logger.debug("index already created skipping creation")
                else:  # Other exception - raise it
                    raise e
        return updated_records

    def _authenticated_client(self) -> elasticsearch.Elasticsearch:
        """
        _authenticated_client generates a newly authenticated elasticsearch client
        attempting to support all auth permutations and ssl concerns

        @return: elasticsearch.Elasticsearch
        """
        config = {}
        scheme = self.config[SCHEME]
        if SSL_CA_FILE in self.config:
            scheme = "https"
            config["ca_certs"] = self.config[SSL_CA_FILE]

        config["hosts"] = [f"{scheme}://{self.config[HOST]}:{self.config[PORT]}"]

        if USERNAME in self.config and PASSWORD in self.config:
            config["basic_auth"] = (self.config[USERNAME], self.config[PASSWORD])
        elif API_KEY in self.config and API_KEY_ID in self.config:
            config["api_key"] = (self.config[API_KEY_ID], self.config[API_KEY])
        elif BEARER_TOKEN in self.config:
            config["bearer_auth"] = self.config[BEARER_TOKEN]
        else:
            self.logger.info("using default elastic search connection config")

        return elasticsearch.Elasticsearch(**config)

    def write_output(self, records):
        # create index
        # build batch request body
        records = self.mapped_body(records)
        # writing to elastic via bulk helper function
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        self.logger.debug(records)
        bulk(self.client, records)

    def process_batch(self, context: dict) -> None:
        records = context["records"]
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
        self.client.close()
