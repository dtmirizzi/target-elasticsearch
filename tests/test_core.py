import os
from typing import Any

from singer_sdk.testing import get_target_test_class
from target_elasticsearch.target import TargetElasticsearch

SAMPLE_CONFIG: dict[str, Any] = {
    "username": "elastic",
    "password": os.environ.get("TARGET_ELASTICSEARCH_PASSWORD", "changeme"),
}

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(target_class=TargetElasticsearch, config=SAMPLE_CONFIG)


class TestTargetElasticsearch(StandardTargetTests):
    """Test class for TargetElasticsearch."""
