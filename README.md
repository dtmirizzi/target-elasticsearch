# target-elasticsearch

![](https://img.shields.io/badge/Maintenance%20Status-Active%20(Stable)-brightgreen)
![](https://img.shields.io/pypi/dm/target-elasticsearch?color=3438BF&label=PyPI%20Downloads&)
![](https://img.shields.io/pypi/v/target-elasticsearch?color=3438BF&label=PyPI%20Package%20Version&)
![](https://github.com/dtmirizzi/target-elasticsearch/actions/workflows/ci_workflow.yml/badge.svg)
![](https://img.shields.io/badge/Total%20Executions%20(Last%203%20Months)-1,052-c0c0c4)
![](https://img.shields.io/badge/Projects%20(Last%203%20Months)-10-c0c0c4)

`target-elasticsearch` is a Singer target for [Elastic](https://www.elastic.co/).

Some target development because I am currently hacking together something together with a bash script
Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Video

[![video](http://img.youtube.com/vi/k6yr8P1f93U/0.jpg)](https://www.youtube.com/watch?v=k6yr8P1f93U "target")


## Settings

| Setting             | Required |                       Default                       | Description                                                                                                                                                                                                                                                                                                                                                                                             |
|:--------------------|:--------:|:---------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scheme              |   True   |                        http                         | http scheme used for connecting to elasticsearch                                                                                                                                                                                                                                                                                                                                                        |
| host                |   True   |                      localhost                      | host used to connect to elasticsearch                                                                                                                                                                                                                                                                                                                                                                   |
| port                |   True   |                        9200                         | port use to connect to elasticsearch                                                                                                                                                                                                                                                                                                                                                                    |
| username            |  False   |                        None                         | basic auth username [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html##auth-basic)                                                                                                                                                                                                                                                 |
| password            |  False   |                        None                         | basic auth password [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html##auth-basic)                                                                                                                                                                                                                                                 |
| bearer_token        |  False   |                        None                         | Bearer token for bearer authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-bearer)                                                                                                                                                                                                                               |
| api_key_id          |  False   |                        None                         | api key id for auth key authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey)                                                                                                                                                                                                                               |
| api_key             |  False   |                        None                         | api key for auth key authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey)                                                                                                                                                                                                                                  |
| ssl_ca_file         |  False   |                        None                         | location of the the SSL certificate for cert verification ie. `/some/path` [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#_verifying_https_with_ca_certificates)                                                                                                                                                                |
| index_format        |  False   | ecs-{{ stream_name }}-{{ current_timestamp_daily }} | can be used to handle custom index formatting such as specifying `-latest` index. Default options: Daily `{{ current_timestamp_daily }}`, Monthly `{{ current_timestamp_monthly }}`, or Yearly `{{ current_timestamp_yearly }}`. You should use fields specified in `index_schema_fields` such as `{{ _id }}` or `{{ timestamp }}` . There are also helper fuctions such as {{ to_daily(timestamp) }}`. |
| index_schema_fields |  False   |                        None                         | this id map allows you to specify specific record values via jsonpath from the stream to be used in index formulation.                                                                                                                                                                                                                                                                                  |
| metadata_fields     |  false   |                        None                         | this should be used to pull out specific fields via jsonpath to be used on for [ecs metadata patters](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html)                                                                                                                                                                                                              |
| index_mappings      |  false   |                        None                         | Define field mappings for each stream/index. Creates or updates Elasticsearch index mappings with specified field types and properties. Format: `{"stream_name": {"properties": {"field_name": {"type": "text"}}}}`. See [MAPPING_EXAMPLES.md](./MAPPING_EXAMPLES.md) for detailed examples.                                                                                                              |
| request_timeout     |  false   |                        10                         | increase timeout to send big butches of data [Elasticsearch connection arguments](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/config.html)                                                                                                                                                                                                              |
| retry_on_timeout     |  false   |                        True                         | increase timeout to send big butches of data [Elasticsearch connection arguments](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/config.html)                                                                                                                                                                                                              |

A full list of supported settings and capabilities is available by running: `target-elasticsearch --about`

## Installation

```bash
git clone git@github.com:dtmirizzi/target_elasticsearch.git
cd target_elasticsearch
pipx install .
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
target_elasticsearch --about
```

### Index Mappings (NEW)

The `index_mappings` configuration allows you to define field mappings for each stream/index, enabling:

- **Better Performance**: Proper field types improve query performance
- **Storage Optimization**: Appropriate field types reduce storage requirements
- **Enhanced Search**: Text analysis and keyword fields enable better search capabilities
- **Data Integrity**: Field types enforce data consistency

#### Example Configuration

```yaml
config:
  # ... other settings ...
  index_mappings:
    users:
      properties:
        email:
          type: keyword
        created_at:
          type: date
        full_name:
          type: text
          analyzer: standard
    orders:
      properties:
        order_id:
          type: keyword
        total_amount:
          type: scaled_float
          scaling_factor: 100
```

For detailed examples and advanced usage, see [MAPPING_EXAMPLES.md](./MAPPING_EXAMPLES.md).

### Target Authentication and Authorization


You can easily run `target-elasticsearch` by itself or in a pipeline using [Meltano](https://meltano.com/).

There are 4 types of auth supported by this target:
- Basic Auth
- Access Token
- Bearer Token
- SSL Based

### Executing the Tap Directly

```bash
target_elasticsearch --version
target_elasticsearch --help
target_elasticsearch --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
poetry run pre-commit install
```

### Create and Run Tests

1. Create tests within the `target_elasticsearch/tests` subfolder
1. To Run tests, first set environment variables in your current shell for all required settings from the [Settings](#Settings) section above. Example for UNIX base systems running `export Target_elasticsearch_USER_ID="xxxxx"` then run:

```bash
poetry run pytest
```

You can also test the `target-elasticsearch` CLI interface directly using `poetry run`:

```bash
poetry run target_elasticsearch --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-elasticsearch
meltano install
```

```bash
# You can spin up elastic locally
make local-es
```

Now you can test and orchestrate using Meltano:

```bash
meltano install
# Test invocation:
meltano invoke target-elasticsearch --version
# test configuration
meltano config target-elasticsearch set --interactive
# OR run a test `elt` pipeline:
meltano elt tap-smoke-test target-elasticsearch
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
