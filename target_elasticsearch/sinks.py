import elasticsearch
import elasticsearch.helpers
import jinja2
import re

from typing import Optional, Union, Any, Tuple, Set

import jsonpath_ng
from dateutil.parser import parse
from elasticsearch.helpers import bulk
from singer_sdk import Target
from singer_sdk.sinks import BatchSink

import datetime

ELASTIC_YEARLY_FORMAT = "%Y"
ELASTIC_MONTHLY_FORMAT = "%Y.%m"
ELASTIC_DAILY_FORMAT = "%Y.%m.%d"


class ElasticSink(BatchSink):
    """Elasticsearch target sink class for batch processing records."""

    MAX_SIZE_DEFAULT = 1000

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Optional[list[str]],
    ):
        super().__init__(target, stream_name, schema, key_properties)
        self.client = self._authenticated_client()
        self.index_schema_fields = self.config.get("index_schema_fields", {}).get(
            self.stream_name, {}
        )
        self.metadata_fields = self.config.get("metadata_fields", {}).get(self.stream_name, {})
        self.index_mappings = self.config.get("index_mappings", {}).get(self.stream_name, {})
        self.index_name = None

    def setup(self) -> None:
        """Perform any setup actions at the beginning of a Stream.

        Setup is executed once per Sink instance, after instantiation. If a Schema
        change is detected, a new Sink is instantiated and this method is called again.
        """
        self.logger.info("Setting up %s", self.stream_name)
        if not self.index_schema_fields:
            self.index_name = self._template_index()
            self.create_index(self.index_name)

    def _template_index(self, schemas: dict = {}) -> str:
        """Template the input index config for Elasticsearch indexing.

        Currently operates using current time as index. This may not be optimal
        and additional support can be added to parse @timestamp out and use it
        in index templating depending on how your elastic instance is configured.

        Args:
            schemas: Dictionary of schema values for templating.

        Returns:
            Templated index name as string.
        """
        today = datetime.date.today()
        arguments = {
            **{
                "stream_name": self.stream_name,
                "current_timestamp_daily": today.strftime(ELASTIC_DAILY_FORMAT),
                "current_timestamp_monthly": today.strftime(ELASTIC_MONTHLY_FORMAT),
                "current_timestamp_yearly": today.strftime(ELASTIC_YEARLY_FORMAT),
                "to_daily": lambda date: parse(date).date().strftime(ELASTIC_DAILY_FORMAT),
                "to_monthly": lambda date: parse(date).date().strftime(ELASTIC_MONTHLY_FORMAT),
                "to_yearly": lambda date: parse(date).date().strftime(ELASTIC_YEARLY_FORMAT),
            },
            **schemas,
        }
        environment = jinja2.Environment()
        template = environment.from_string(self.config["index_format"])
        return re.sub(r"[^a-z0-9-]+", "", template.render(**arguments).replace("_", "-").lower())

    def _build_fields(
        self,
        mapping: dict,
        record: dict[str, Union[str, dict[str, str], int]],
    ) -> dict:
        """Parse records for supplied mapping to be used in index templating and ECS metadata field formulation.

        Args:
            mapping: Dictionary mapping configuration.
            record: Input record to parse.

        Returns:
            Dictionary of parsed schema fields.
        """
        schemas = {}
        self.logger.debug("index_schema_fields", ": ", mapping)
        for k, v in mapping.items():
            match = jsonpath_ng.parse(v).find(record)
            if len(match) == 0:
                self.logger.warning(
                    f"schema key {k} with json path {v} could not be found in record: {record}"
                )
                schemas[k] = v
            else:
                if len(match) > 1:
                    self.logger.warning(
                        f"schema key {k} with json path {v} has multiple associated fields, may cause side effects"
                    )
                schemas[k] = match[0].value
        return schemas

    def build_request_body_and_distinct_indices(
        self, records: list[dict[str, Union[str, dict[str, str], int]]]
    ) -> Tuple[list[dict[Union[str, Any], Union[str, Any]]], Set[str]]:
        """Build the bulk request body and collect distinct indices.

        Builds the bulk request body and collects all distinct indices that will
        be used to create indices before bulk upload.

        Args:
            records: List of records to process.

        Returns:
            Tuple containing the updated records list and set of distinct indices.
        """
        updated_records = []
        distinct_indices = set()

        for record in records:
            if self.index_schema_fields:
                index = self._template_index(self._build_fields(self.index_schema_fields, record))
                distinct_indices.add(index)
            else:
                index = self.index_name
            updated_record = {"_op_type": "index", "_index": index, "_source": record}
            if self.metadata_fields is not None:
                # Build metadata fields for the record
                metadata_fields = self._build_fields(self.metadata_fields, record)
                updated_record.update(metadata_fields)
            updated_records.append(updated_record)

        return updated_records, distinct_indices

    def create_index(self, index: str) -> None:
        """Create Elasticsearch indices using cluster defaults or configured mappings.

        Args:
            indices: Set of index names to create.
        """
        if self.client.indices.exists(index=index):
            if self.index_mappings:
                mappings = {
                    key: value["mapping"][key]["type"]
                    for key, value in self.client.indices.get_field_mapping(
                        index=index, fields=self.index_mappings.keys()
                    )["mappings"].items()
                }
                if not all(self.index_mappings[key] == value for key, value in mappings):
                    self.logger.warning(
                        f"Index {index} already exists with different mappings. Recreate index with new mappings."
                    )
                elif mappings.keys() != self.index_mappings.keys():
                    self.logger.info(
                        f"Index {index} exists but with different fields. Updating mapping for existing index."
                    )
                    self.client.indices.put_mapping(index=index, body=self.index_mappings)
            else:
                self.logger.debug(f"Index {index} already exists, skipping creation.")
        else:
            self.logger.info(f"Creating index {index} with mappings: {self.index_mappings}")
            self.client.indices.create(index=index, mappings=self.index_mappings)

    def _authenticated_client(self) -> elasticsearch.Elasticsearch:
        """Generate a newly authenticated Elasticsearch client.

        Attempts to support all auth permutations and SSL concerns.
        See: https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html

        Returns:
            Configured Elasticsearch client instance.
        """
        config = {}
        scheme = self.config["scheme"]
        if self.config.get("ssl_ca_file"):
            scheme = "https"
            config["ca_certs"] = self.config.get("ssl_ca_file")

        config["hosts"] = [f"{scheme}://{self.config['host']}:{self.config['port']}"]
        config["request_timeout"] = self.config["request_timeout"]
        config["retry_on_timeout"] = self.config["retry_on_timeout"]

        if self.config.get("username") and self.config.get("password"):
            config["basic_auth"] = (self.config["username"], self.config["password"])
        elif self.config.get("api_key") and self.config.get("api_key_id"):
            config["api_key"] = (self.config["api_key_id"], self.config["api_key"])
        elif self.config.get("encoded_api_key"):
            config["api_key"] = self.config["encoded_api_key"]
        elif self.config.get("bearer_token"):
            config["bearer_auth"] = self.config["bearer_token"]
        else:
            self.logger.info("using default elastic search connection config")

        config["headers"] = {"user-agent": self._elasticsearch_user_agent()}

        return elasticsearch.Elasticsearch(**config)

    def process_batch(self, context: dict[str, Any]) -> None:
        """Handle batch records and override the default sink implementation.

        Args:
            context: Dictionary containing batch processing context including records.
        """
        updated_records, distinct_indices = self.build_request_body_and_distinct_indices(
            context["records"]
        )
        for index in distinct_indices:
            self.create_index(index)
        try:
            bulk(self.client, updated_records)
        except elasticsearch.helpers.BulkIndexError as e:
            self.logger.error(e.errors)

    def clean_up(self) -> None:
        """Close the Elasticsearch client connection."""
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
        self.client.close()

    def _elasticsearch_user_agent(self) -> str:
        """Return a user agent string for the Elasticsearch client.

        Returns:
            User agent string containing package version information.
        """
        return f"meltano-loader-elasticsearch/{Target._get_package_version('target-elasticsearch')}"
