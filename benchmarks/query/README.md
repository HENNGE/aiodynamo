# Query Benchmarks

Benchmarks the query performance of various clients.

## Dependencies

You'll need `aiodynamo`, `boto3`, `aiobotocore`, `botocore`, `pyperf`, `aiohttp` and `httpx` installed.

## Environment

* `BENCH_ENDPOINT_URL`: The "endpoint URL" to connect to, use http://localhost:8000/ for testing against `dynalite` or leave unset for AWS
* `BENCH_TABLE_NAME`: Name of the table to benchmark (must already exist and contain data).
* `BENCH_KEY_FIELD`: Name of the partition key to query.
* `BENCH_KEY_VALUE`: Value of the partition key to query.
* `BENCH_REGION_NAME`: AWS region the table is in.

You'll also need to make sure that the environment has valid credentials set up.

## Run

### boto3

`python bench_boto3.py -o boto3.json --rigorous --inherit-env BENCH_ENDPOINT_URL,BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### botocore

`python bench_botocore.py -o botocore.json --rigorous --inherit-env BENCH_ENDPOINT_URL,BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiobotocore

`python bench_aiobotocore.py -o aiobotocore.json --rigorous --inherit-env BENCH_ENDPOINT_URL,BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiodynamo using aiohttp

`python bench_aiodynamo_aiohttp.py -o aiodynamo_aiohttp.json --rigorous --inherit-env BENCH_ENDPOINT_URL,BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiodynamo using httpx

`python bench_aiodynamo_httpx.py -o aiodynamo_httpx.json --rigorous --inherit-env BENCH_ENDPOINT_URL,BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

## Compare

`pyperf compare_to boto3.json botocore.json aiobotocore.json aiodynamo_aiohttp.json aiodynamo_httpx.json`

## Setup

Ideally, an existing production dynamodb table should be used. If that's not an option, read on:

To create a table, use something like `aws dynamodb [--endpoint-url http://localhost:8000] create-table --table-name="foobar" --key-schema "AttributeName=foobar,KeyType=HASH" "AttributeName=quux,KeyType=RANGE" --attribute-definitions "AttributeName=foobar,AttributeType=S" "AttributeName=quux,AttributeType=S" --provisioned-throughput ReadCapacityUnits=5000,WriteCapacityUnits=5000`

Then, the environment variables may look like:

```
export BENCH_ENDPOINT_URL="http://localhost:8000/"
export BENCH_TABLE_NAME="foobar"
export BENCH_KEY_FIELD="foobar"
export BENCH_KEY_VALUE="foobar"
export BENCH_REGION_NAME="us-east-1"
export BENCH_RANGE_KEY_NAME="quux"
```

A script `fill_db.py` is provided to fill up one shard in this table.
