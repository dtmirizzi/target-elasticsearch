import json

from singer_sdk.sinks import BatchSink


class ElasticSink(BatchSink):
    """ElasticSink target sink class."""

    max_size = 1000  # Max records to write in one batch

    def index(self, ) -> str:
        pass

    def mapped_body(self, index, records) -> [str]:
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
            for k, v in self.config["schema_mapping"][self.stream_name]:
                schemas[k] = r.get(v, None)
            updated_records.append(json.dumps({"index": index, "_source": r} | schemas))

        return updated_records

    # TODO import elasticsearch and handle multiple auth patterns
    def authenticated_client(self) -> any:
        return {}

    def write_output(self, records):
        index = self.index()
        # get index
        self.logger.info(f"Writing elastic index to {index}")
        # build batch request body
        records = self.mapped_body(records)
        # build an authenticated request and send request body
        cli = self.authenticated_client()
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        bulk(cli, records)

    def process_batch(self, context: dict) -> None:
        records = context["records"]
        self.tally_duplicate_merged(len(records) - len(set(records)))
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
