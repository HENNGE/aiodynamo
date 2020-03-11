# Query Benchmarks

Benchmarks the query performance of various clients.

## Dependencies

You'll need `aiodynamo`, `boto3`, `aiobotocore`, `botocore`, `pyperf`, `aiohttp` and `httpx` installed.

## Environment

* `BENCH_TABLE_NAME`: Name of the table to benchmark (must already exist and contain data).
* `BENCH_KEY_FIELD`: Name of the partition key to query.
* `BENCH_KEY_VALUE`: Value of the partition key to query.
* `BENCH_REGION_NAME`: AWS region the table is in.

You'll also need to make sure that the environment has valid credentials set up.

## Run

### boto3

`python bench_boto3.py -o boto3.json --rigorous --inherit-env BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### botocore

`python bench_botocore.py -o botocore.json --rigorous --inherit-env BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiobotocore

`python bench_aiobotocore.py -o aiobotocore.json --rigorous --inherit-env BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiodynamo using aiohttp

`python bench_aiodynamo_aiohttp.py -o aiodynamo_aiohttp.json --rigorous --inherit-env BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

### aiodynamo using httpx

`python bench_aiodynamo_httpx.py -o aiodynamo_httpx.json --rigorous --inherit-env BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`

## Compare

`pyperf compare boto3.json botocore.json aiobotocore.json aiodynamo_aiohttp.json aiodynamo_httpx.json`
