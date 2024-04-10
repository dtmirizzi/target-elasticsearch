import elasticsearch
import jinja2

from typing import List, Dict, Optional, Union, Any, Tuple, Set

import re
import concurrent
import jsonpath_ng
import singer_sdk.io_base
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError
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
    ENCODED_API_KEY,
    SSL_CA_FILE,
    INDEX_TEMPLATE_FIELDS,
    ELASTIC_YEARLY_FORMAT,
    ELASTIC_MONTHLY_FORMAT,
    ELASTIC_DAILY_FORMAT,
    METADATA_FIELDS,
    NAME,
    PREFERRED_PKEY,
    CHECK_DIFF,
    STREAM_NAME,
    EVENT_TIME_KEY,
    IGNORED_FIELDS,
    DEFAULT_IGNORED_FIELDS,
    DIFF_SUFFIX,
    to_daily,
    to_monthly,
    to_yearly,
)


def template_index(stream_name: str, index_format: str, schemas: Dict) -> str:
    """
    _index templates the input index config to be used for elasticsearch indexing
    currently it operates using current time as index.
    this may not be optimal and additional support can be added to parse @timestamp out and use it in index
    templating depending on how your elastic instance is configured.
    @param stream_name:
    @param index_format:
    @param schemas:
    @return: str
    """
    today = datetime.date.today()
    arguments = {
        **{
            "stream_name": stream_name,
            "current_timestamp_daily": today.strftime(ELASTIC_DAILY_FORMAT),
            "current_timestamp_monthly": today.strftime(ELASTIC_MONTHLY_FORMAT),
            "current_timestamp_yearly": today.strftime(ELASTIC_YEARLY_FORMAT),
            "to_daily": to_daily,
            "to_monthly": to_monthly,
            "to_yearly": to_yearly,
        },
        **schemas,
    }
    environment = jinja2.Environment()
    template = environment.from_string(index_format)
    return template.render(**arguments).replace("_", "-")


def build_fields(
    stream_name: str,
    mapping: Dict,
    record: Dict[str, Union[str, Dict[str, str], int]],
    logger: singer_sdk.io_base.logger,
) -> Dict:
    """
    build_fields parses records for supplied mapping to be used later in index templating and ecs metadata field formulation
    @param logger:
    @param stream_name:
    @param mapping: dict
    @param record:  str
    @return: dict
    """
    schemas = {}
    if stream_name in mapping:
        logger.debug(INDEX_TEMPLATE_FIELDS, ": ", mapping[stream_name])
        for k, v in mapping[stream_name].items():
            match = jsonpath_ng.parse(v).find(record)
            if len(match) == 0:
                logger.warning(
                    f"schema key {k} with json path {v} could not be found in record: {record}"
                )
                schemas[k] = v
            else:
                if len(match) < 1:
                    logger.warning(
                        f"schema key {k} with json path {v} has multiple associated fields, may cause side effects"
                    )
                schemas[k] = match[0].value
    return schemas


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

    def build_request_body_and_distinct_indices(
        self, records: List[Dict[str, Union[str, Dict[str, str], int]]]
    ) -> Tuple[List[Dict[Union[str, Any], Union[str, Any]]], Set[str]]:
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
        diff_enabled = [elem for elem in self.config[CHECK_DIFF] if re.search(elem[STREAM_NAME], self.stream_name)]
        self.logger.info(f"Diff enabled for stream {self.stream_name}:: {len(diff_enabled) > 0}")
        for r in records:
            index = template_index(
                self.stream_name,
                self.config[INDEX_FORMAT],
                build_fields(self.stream_name, index_mapping, r, self.logger),
            )
            distinct_indices.add(index)
            
            doc_id = self.build_doc_id(self.stream_name, r)
            if doc_id != "":
                # Upsert logic:
                # ctx.op == create => If the document does not exist, insert r including _sdc_sequence
                # Else, if the document exists, update with all fields from r except _sdc_sequence
                updated_records.append({
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "scripted_upsert": True,
                    "script": {
                        "source": """
                        if (ctx.op == 'create') {
                            ctx._source.putAll(params.r);
                        } else {
                            for (entry in params.r.entrySet()) {
                                if (!entry.getKey().equals('_sdc_sequence')) { // Skip _sdc_sequence field
                                    ctx._source.put(entry.getKey(), entry.getValue());
                                }
                            }
                        }
                        """,
                        "lang": "painless",
                        "params": {
                            "r": {
                                **r,
                                **build_fields(self.stream_name, metadata_fields, r, self.logger)
                            }
                        }
                    },
                    "upsert": {}  # Empty document for upsert; actual content is managed by the script
                })
            else:
                # Default insertion
                updated_records.append(
                    {
                        **{"_op_type": "index", "_index": index, "_source": r},
                        **build_fields(self.stream_name, metadata_fields, r, self.logger),
                    }
                )

        if len(diff_enabled) > 0 and len(records) > 0:
            index = template_index(
                self.stream_name,
                self.config[INDEX_FORMAT],
                build_fields(self.stream_name, index_mapping, r, self.logger),
            )
            # Diff processing is something which can take a long time, as queries need to be made
            # -> Parallelize these checks
            self.logger.info(f"Generate diff records based on the {len(records)} new records")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self.process_main_records_make_diffs, record, index, diff_enabled, metadata_fields)
                    for record in records]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result is not None:
                        updated_records.append(result)

        return updated_records, distinct_indices

    def create_indices(self, indices: Set[str]) -> None:
        """
        create_indices creates elastic indices using cluster defaults
        @param indices: set
        """
        for index in indices:
            try:
                self.client.indices.create(
                    index=index,
                    settings={
                        "index": {
                            "mapping": {
                                # Do not raise an error if the schema is not perfect - eg. empty string for a null date
                                "ignore_malformed": True,
                                "total_fields": {
                                    # Default value is 1000, but diff events may get quite big
                                    "limit": 2000
                                }
                            }
                        }
                    }
            )
            except elasticsearch.exceptions.RequestError as e:
                if e.error == "resource_already_exists_exception":
                    self.logger.debug("index already created skipping creation")
                else:  # Other exception - raise it
                    raise e

    def build_body(
        self, records: List[Dict[str, Union[str, Dict[str, str], int]]]
    ) -> List[Dict[Union[str, Any], Union[str, Any]]]:
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
        elif ENCODED_API_KEY in self.config:
            config["api_key"] = self.config[ENCODED_API_KEY]
        elif BEARER_TOKEN in self.config:
            config["bearer_auth"] = self.config[BEARER_TOKEN]
        else:
            self.logger.info("using default elastic search connection config")

        config["headers"] = {"user-agent": self._elasticsearch_user_agent()}

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

    def process_batch(self, context: Dict[str, Any]) -> None:
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

    def _elasticsearch_user_agent(self) -> str:
        """
        Returns a user agent string for the elasticsearch client
        """
        return f"meltano-loader-elasticsearch/{PluginBase._get_package_version(NAME)}"

    def build_doc_id(self, stream_name: str, r: Dict[str, Union[str, Dict[str, str], int]]) -> str:
        # 1. Explicitly handled cases
        if stream_name in PREFERRED_PKEY:
            if False not in [x in r for x in PREFERRED_PKEY[stream_name]]:
                return "-".join([str(r[x]) for x in PREFERRED_PKEY[stream_name]])

        # 2. Best effort to avoid duplicates:
        # In descending priority, try to match a field which looks like a primary key
        id_fields = ["id", "ID", "Id", "accountId", "sha", "hash", "node_id", "idx", "key", "ts"]
        for id_field in id_fields:
            if id_field in r:
                return r[id_field]
            
        return ""
    
    def process_main_records_make_diffs(self, r, index, diff_enabled, metadata_fields):
        doc_id = self.build_doc_id(self.stream_name, r)
        if doc_id == "":
            return None
        diff_event, ignore = self.process_diff_event(index, doc_id, r, diff_enabled[0])
        if not ignore:
            # Default insertion: no need for an update in the case of events
            self.logger.debug(f"Append event for stream {index+DIFF_SUFFIX}: {diff_event}")
            return {
                **{"_op_type": "index", "_index": index+DIFF_SUFFIX, "_source": diff_event, "_id": diff_event["id"]},
                **build_fields(self.stream_name+DIFF_SUFFIX, metadata_fields, diff_event, self.logger),
            }
        return None

    def process_diff_event(self, main_index: str, doc_id: str, new_doc: Dict[str, str | Dict[str, str] | int], diff_config: Dict) -> Tuple[Dict, bool]:
        # Raise an exception if the field does not exist in the doc
        if diff_config.get(EVENT_TIME_KEY, '').lower() == "autogenerated":
            evt_time = datetime.datetime.now().isoformat()
        else:
            evt_time = new_doc.get(diff_config.get(EVENT_TIME_KEY))
        ignored_fields = []
        ignored_fields.extend(diff_config.get(IGNORED_FIELDS, []))
        ignored_fields.extend(DEFAULT_IGNORED_FIELDS)

        diff_event = {}
        diff_event["id"] = f"{doc_id}-event-{evt_time}"
        diff_event["main_doc_key"] = doc_id
        diff_event["event_ts"] = evt_time
        # Inherit the sdc sequence from the new doc
        diff_event["_sdc_sequence"] = new_doc.get("_sdc_sequence")
        diff_event["_sdc_extracted_at"] = new_doc.get("_sdc_extracted_at")
        diff_event["_sdc_batched_at"] = new_doc.get("_sdc_batched_at")

        original_doc_exists = True
        try:
            res = self.client.get(index=main_index, id=doc_id)
            original_doc = res["_source"]
        except NotFoundError:
            original_doc_exists = False
        except Exception as e:
            # Should not happen -> raise
            self.logger.error(f"Error while fetching document {doc_id} from {main_index} in order to build diff: {e}")
            raise e
        
        if not original_doc_exists:
            original_doc = {}

        diff_result = dict_diff(original_doc, new_doc, ignored_fields)
        diff_event["from"] = diff_result["from"]
        diff_event["to"] = diff_result["to"]
        # Also include the full docs, to make future queries easier (it's slow if we need to access the original doc for context each time)
        # However, only include the resulting doc because otherwise the diff doc would be too heavy!
        # diff_event["original_doc"] = original_doc
        diff_event["new_doc"] = new_doc

        ignore = False
        if len(diff_event["from"].keys()) == 0 and len(diff_event["to"].keys()) == 0:
            ignore = True

        diff_event = {k: v for k, v in diff_event.items() if v != None and v != ""}

        return diff_event, ignore
    

def dict_diff(old, new, ignored_fields):
    diff = {"from": {}, "to": {}}

    all_keys = set(old.keys()) | set(new.keys())

    for key in all_keys:
        if any(re.match(pattern, key) for pattern in ignored_fields):
            continue
        if key in old and key in new:
            # If the value is a dictionary, compare recursively
            if isinstance(old[key], dict) and isinstance(new[key], dict):
                nested_diff = dict_diff(old[key], new[key], ignored_fields)
                if nested_diff["from"] or nested_diff["to"]:  # If there's a change
                    diff["from"][key] = nested_diff["from"]
                    diff["to"][key] = nested_diff["to"]
            elif old[key] != new[key]:  # If the value has changed
                diff["from"][key] = old[key]
                diff["to"][key] = new[key]
        elif key in new:  # If the key is an addition
            diff["to"][key] = new[key]
        elif key in old:  # If the key is a removal
            diff["from"][key] = old[key]

    return diff
