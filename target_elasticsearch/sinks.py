import json
from typing import List

import elasticsearch

from elasticsearch.helpers import bulk
from singer_sdk.sinks import BatchSink

from target_elasticsearch.target import (
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


class ElasticSink(BatchSink):
    """ElasticSink target sink class."""

    max_size = 1000  # Max records to write in one batch

    # TODO Define templating
    def index(
        self,
    ) -> str:
        pass

    def mapped_body(self, index, records) -> List[str]:
        """
        mapped_body maps config schemas to record schemas
        by default the record will be placed into _source and _id and @timestamp will be left for elastic to generate
        [{
        _id: "config.schema_mapping[stream]['_id'](json path in record) || es auto generated",
        config.schema_mapping[stream][#]: "config.schema_mapping[stream][#](json path in record)"  || None ,
        _source: {
        .record
        }
        }]
        * Not really loving this implementation *
        :param records:
        :return: [str]
        """

        updated_records = []
        for r in records:
            schemas = {}
            for k, v in self.config[SCHEMA_MAPPING][self.stream_name]:
                schemas[k] = r.get(v, None)
            updated_records.append(json.dumps({"index": index, "_source": r} | schemas))

        return updated_records

    # TODO import elasticsearch and handle multiple auth patterns
    def authenticated_client(self) -> elasticsearch.Elasticsearch:
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
        index = self.index()
        # build batch request body
        records = self.mapped_body(index, records)
        # build an authenticated request and send request body
        cli = self.authenticated_client()
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        self.logger.info(f"Writing elastic index to {index}")
        bulk(cli, records)

    def process_batch(self, context: dict) -> None:
        records = context["records"]
        self.tally_duplicate_merged(len(records) - len(set(records)))
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
