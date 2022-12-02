import json

import elasticsearch
import jinja2

from typing import List, Dict, Optional

import jsonpath_ng
from elasticsearch.helpers import bulk
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink

import datetime

from target_elasticsearch.target import (
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

ELASTIC_YEARLY_FORMAT = "%Y"
ELASTIC_MONTHLY_FORMAT = "%Y.%m"
ELASTIC_DAILY_FORMAT = "%Y.%m.%D"


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

    def index(self, schemas: dict) -> str:
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
        return template.render(**arguments)

    def mapped_body(self, records: List[str]) -> List[str]:
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
        for r in records:
            schemas = {}
            for k, v in self.config[SCHEMA_MAPPING][self.stream_name]:
                expression = jsonpath_ng.parse(v)
                match = expression.find(json.loads(r))
                if len(match) == 0:
                    self.logger.warning(
                        f"schema key {k} with json path {v} could not be found for record"
                    )
                else:
                    schemas[k] = match
            index = self.index(schemas)
            updated_records.append(json.dumps({"index": index, "_source": r} | schemas))

        return updated_records

    # TODO import elasticsearch and handle multiple auth patterns
    def _authenticated_client(self) -> elasticsearch.Elasticsearch:
        """
        _authenticated_client generates a newly authenticated elasticsearch client
        attempting to support all auth permutations and ssl concerns
        @return: elasticsearch.Elasticsearch
        """
        scheme = self.config[SCHEME]
        if self.config[SSL_CA_FILE]:
            scheme = "https"

        endpoint = f"{scheme}://{self.config[HOST]}:{self.config[PORT]}"
        config = {"hosts": [endpoint]}

        if self.config[USERNAME] and self.config[PASSWORD]:
            pass
        elif self.config[API_KEY] and self.config[API_KEY_ID]:
            pass
        elif self.config[BEARER_TOKEN]:
            pass
        else:
            self.logger.info("using default elastic search connection config")

        return elasticsearch.Elasticsearch(**config)

    def write_output(self, records):
        # build batch request body
        records = self.mapped_body(records)
        # writing to elastic via bulk helper function
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        bulk(self.client, records)

    def process_batch(self, context: dict) -> None:
        records = context["records"]
        self.tally_duplicate_merged(len(records) - len(set(records)))
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
