from dateutil.parser import parse

ELASTIC_YEARLY_FORMAT = "%Y"
ELASTIC_MONTHLY_FORMAT = "%Y.%m"
ELASTIC_DAILY_FORMAT = "%Y.%m.%d"

SCHEME = "scheme"
HOST = "host"
PORT = "port"
USERNAME = "username"
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
REQUEST_TIMEOUT = "request_timeout"
RETRY_ON_TIMEOUT = "retry_on_timeout"

DEFAULT_REQUEST_TIMEOUT = 10
DEFAULT_RETRY_ON_TIMEOUT = True


def to_daily(date) -> str:
    return parse(date).date().strftime(ELASTIC_DAILY_FORMAT)


def to_monthly(date) -> str:
    return parse(date).date().strftime(ELASTIC_MONTHLY_FORMAT)


def to_yearly(date) -> str:
    return parse(date).date().strftime(ELASTIC_YEARLY_FORMAT)
