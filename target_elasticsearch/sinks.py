import elasticsearch
import jinja2

from typing import List, Dict, Optional, Union, Any, Tuple, Set

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
    INDEX_TEMPLATE_FIELDS,
    ELASTIC_YEARLY_FORMAT,
    ELASTIC_MONTHLY_FORMAT,
    ELASTIC_DAILY_FORMAT,
    METADATA_FIELDS,
    to_daily,
    to_monthly,
    to_yearly,
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
            "to_daily": to_daily,
            "to_monthly": to_monthly,
            "to_yearly": to_yearly,
        } | schemas
        environment = jinja2.Environment()
        template = environment.from_string(self.config[INDEX_FORMAT])
        return template.render(**arguments).replace("_", "-")

    def build_fields(self, mapping: dict, record: str) -> dict:
        """
        build_fields parses records for supplied mapping to be used later in index templating and ecs metadata field formulation
        @param mapping: dict
        @param record:  str
        @return: dict
        """
        schemas = {}
        if self.stream_name in mapping:
            self.logger.debug(INDEX_TEMPLATE_FIELDS, ": ", mapping[self.stream_name])
            for k, v in mapping[self.stream_name].items():
                expression = jsonpath_ng.parse(v)
                match = expression.find(record)
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
        return schemas

    def build_request_body_and_distinct_indices(
        self, records: List[str]
    ) -> Tuple[List[dict[Union[str, Any], Union[str, Any]]], Set[str]]:
        """
        build_request_body_and_distinct_indices builds the bulk request body
        and collects all distinct indices that will be used to create indices before bulk upload.
        @param records:
        @return:
        """
        updated_records = []
        index_mapping = {}
        metadata_fields = {}
        distinct_indices = set()
        if INDEX_TEMPLATE_FIELDS in self.config:
            index_mapping = self.config[INDEX_TEMPLATE_FIELDS]
        if METADATA_FIELDS in self.config:
            metadata_fields = self.config[METADATA_FIELDS]

        for r in records:
            index = self.template_index(self.build_fields(index_mapping, r))
            distinct_indices.add(index)
            updated_records.append(
                {"_op_type": "index", "_index": index, "_source": r}
                | self.build_fields(metadata_fields, r)
            )

        return updated_records, distinct_indices

    def create_indices(self, indices: set) -> None:
        """
        create_indices creates elastic indices using cluster defaults
        @param indices: set
        """
        for index in indices:
            try:
                self.client.indices.create(index=index)
            except elasticsearch.exceptions.RequestError as e:
                if e.error == "resource_already_exists_exception":
                    self.logger.debug("index already created skipping creation")
                else:  # Other exception - raise it
                    raise e

    def build_body(self, records: List[str]) -> list[dict[Union[str, Any], Union[str, Any]]]:
        """
        build_body constructs the bulk message body and creates all necessary indices if needed
        @param records: str
        @return: list[dict[Union[str, Any], Union[str, Any]]]
        """
        updated_records, distinct_indices = self.build_request_body_and_distinct_indices(records)
        self.create_indices(distinct_indices)
        return updated_records

    def _authenticated_client(self) -> elasticsearch.Elasticsearch:
        """
        _authenticated_client generates a newly authenticated elasticsearch client
        attempting to support all auth permutations and ssl concerns
        https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html
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
        """
        write_output creates indices, builds batch request body, and writing to elastic via bulk helper function
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        @param records:
        """
        records = self.build_body(records)
        self.logger.debug(records)
        try:
            bulk(self.client, records)
        except elasticsearch.helpers.BulkIndexError as e:
            self.logger.error(e.errors)

    def process_batch(self, context: dict) -> None:
        """
        process_batch handles batch records and overrides the default sink implementation
        @param context: dict
        """
        records = context["records"]
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        """
        clean_up closes the elasticsearch client
        """
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
        self.client.close()
