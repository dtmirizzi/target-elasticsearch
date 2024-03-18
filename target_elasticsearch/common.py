from dateutil.parser import parse

ELASTIC_YEARLY_FORMAT = "%Y"
ELASTIC_MONTHLY_FORMAT = "%Y.%m"
ELASTIC_DAILY_FORMAT = "%Y.%m.%d"

SCHEME = "scheme"
HOST = "host"
USERNAME = "username"
PORT = "port"
PASSWORD = "password"
BEARER_TOKEN = "bearer_token"
API_KEY_ID = "api_key_id"
API_KEY = "api_key"
ENCODED_API_KEY = "encoded_api_key"
SSL_CA_FILE = "ssl_ca_file"
INDEX_FORMAT = "index_format"
INDEX_TEMPLATE_FIELDS = "index_schema_fields"
METADATA_FIELDS = "metadata_fields"
NAME = "target-elasticsearch"
PREFERRED_PKEY = {
    "channel_members": ("member_id", "channel_id"),
    "messages": ("client_msg_id"),
    "threads": ("client_msg_id")
}
CHECK_DIFF = "check_diff"
DIFF_SUFFIX = "-diff-events"
STREAM_NAME = "stream_name"
EVENT_TIME_KEY = "event_time_key"
IGNORED_FIELDS = "ignored_fields"
DEFAULT_IGNORED_FIELDS = [
    "createdAt",
    "updatedAt",
    "_sdc_extracted_at",
    "_sdc_sequence",
    "_sdc_batched_at",
    "_sdc_received_at",
    "_sdc_sync_started_at",
]


def to_daily(date) -> str:
    return parse(date).date().strftime(ELASTIC_DAILY_FORMAT)


def to_monthly(date) -> str:
    return parse(date).date().strftime(ELASTIC_MONTHLY_FORMAT)


def to_yearly(date) -> str:
    return parse(date).date().strftime(ELASTIC_YEARLY_FORMAT)
