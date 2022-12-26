import datetime
import unittest
from unittest import mock

import singer_sdk.io_base
import time_machine
from target_elasticsearch.sinks import template_index, build_fields


def test_template_index():
    dt = datetime.datetime(2017, 11, 28, 23, 55, 59, 342380)
    assert "" == template_index("", "", {})
    assert "animals-latest" == template_index("animals", "{{ stream_name }}-latest", {})
    assert "animals-latest" == template_index(
        "animals",
        "{{ stream_name }}-latest",
        {"timestamp": dt.isoformat()},
    )
    assert "animals-2017" == template_index(
        "animals",
        "{{ stream_name }}-{{ to_yearly(timestamp) }}",
        {"timestamp": dt.isoformat()},
    )
    assert "animals-2017.11" == template_index(
        "animals",
        "{{ stream_name }}-{{ to_monthly(timestamp) }}",
        {"timestamp": dt.isoformat()},
    )
    assert "2017.11.28" == template_index(
        "animals",
        "{{ to_daily(timestamp) }}",
        {"timestamp": dt.isoformat()},
    )
    with time_machine.travel(dt):
        assert "2017.11.28" == template_index(
            "",
            "{{ current_timestamp_daily }}",
            {},
        )
        assert "2017.11" == template_index(
            "",
            "{{ current_timestamp_monthly }}",
            {},
        )
        assert "2017" == template_index(
            "",
            "{{ current_timestamp_yearly }}",
            {},
        )


def test_build_fields():
    logger = singer_sdk.io_base.logger
    assert {} == build_fields("", {}, {}, logger)
    record = {
        "id": 1,
        "created_at": "some tz",
        "some_nesting": {"test": "bar"},
        "some_array": ["biz", "buz"],
    }
    assert {"timestamp": "some tz"} == build_fields(
        "animals", {"animals": {"timestamp": "created_at"}}, record, logger
    )
    assert {"hup": "bar"} == build_fields(
        "animals", {"animals": {"hup": "some_nesting.test"}}, record, logger
    )
    assert {"hup": "biz"} == build_fields(
        "animals", {"animals": {"hup": "some_array[0]"}}, record, logger
    )


class SinkTests(unittest.TestCase):
    @mock.patch("target_elasticsearch.sinks.ElasticSink")
    def test_config(self, mock_es):
        pass
