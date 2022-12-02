# target-elasticsearch [DRAFT]

`target-elasticsearch` ![](https://github.com/dtmirizzi/target-elasticsearch/actions/workflows/ci_workflow.yml/badge.svg) is a Singer target for [Elastic](https://www.elastic.co/).

Some target development because I am currently hacking together something together with a bash script
Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Capabilities

* `about`

## Settings

| Setting        | Required |                     Default                     | Description                                                                                                                                                                                                                                                                                                                      |
|:---------------|:--------:|:-----------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scheme         |   True   |                      http                       | http scheme used for connecting to elasticsearch                                                                                                                                                                                                                                                                                 |
| host           |   True   |                    localhost                    | host used to connect to elasticsearch                                                                                                                                                                                                                                                                                            |
| port           |   True   |                      9200                       | port use to connect to elasticsearch                                                                                                                                                                                                                                                                                             |
| username       |  False   |                      None                       | basic auth username [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html##auth-basic)                                                                                                                                                                          |
| password       |  False   |                      None                       | basic auth password [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html##auth-basic)                                                                                                                                                                          |
| bearer_token   |  False   |                      None                       | Bearer token for bearer authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-bearer)                                                                                                                                                        |
| api_key_id     |  False   |                      None                       | api key id for auth key authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey)                                                                                                                                                        |
| api_key        |  False   |                      None                       | api key for auth key authorization [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#auth-apikey)                                                                                                                                                           |
| ssl_ca_file    |  False   |                      None                       | location of the the SSL certificate for cert verification ie. `/some/path` [as per elastic documentation](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html#_verifying_https_with_ca_certificates)                                                                                         |
| index_format   |  False   | {{ stream_name }}-{{ current_timestamp_daily }} | can be used to handle custom index formatting such as specifying `-latest` index. Default options: Daily `{{ current_timestamp_daily }}`, Monthly `{{ current_timestamp_monthly }}`, or Yearly `{{ current_timestamp_yearly }}`. Also you can use fields specified in `schema_mapping` such as `{{ _id }}` or `{{ @timestamp }}` |
| schema_mapping |  False   |                      None                       | this id map allows you to specify specific record values from the stream to be used as ECS schema types such as _id. This is not performant, but was built to handle basic extract-load cases and should not be used for complex translations.                                                                                   |

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

### Source Authentication and Authorization


You can easily run `target-elasticsearch` by itself or in a pipeline using [Meltano](https://meltano.com/).


### Executing the Tap Directly

```bash
target_elasticsearch --version
target_elasticsearch --help
target_elasticsearch --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Release Process
For each release we will create a version following [semver](https://semver.org/)

GitHub Steps
1. Merge all changes you'd like to be in release to main
1. Update pyproject.toml with your released version ie 0.0.2
1. Go to github repo click create a release
1. Create a tag for the release (tag will be in the form "v{version}" ie "v0.0.2" release


### Initialize your Development Environment

```bash
pipx install poetry
poetry install
poetry run pre-commit install
```

### Create and Run Tests

1. Create tests within the `tap_elasticsearch/tests` subfolder
1. To Run tests, first set environment variables in your current shell for all required settings from the [Settings](#Settings) section above. Example for UNIX base systems running `export TAP_elasticsearch_USER_ID="xxxxx"` then run:

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
cd target_elasticsearch
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target_elasticsearch --version
# OR run a test `elt` pipeline:
meltano elt tap-gitlab target_elasticsearch
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
