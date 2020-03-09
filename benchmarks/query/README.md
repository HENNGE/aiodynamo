Run:

* `BENCH_TABLE_NAME=<table> BENCH_KEY_FIELD=<key-field> BENCH_KEY_VALUE=<key-value> BENCH_REGION_NAME=<region-name> poetry run  python bench_<aiobotocore|aiodynamo_aiohttp|aiodynamo_httpx|boto3|botocore>.py -o <aiobotocore|aiodynamo_aiohttp|aiodynamo_httpx|boto3|botocore>.json --rigorous --inherit-environ BENCH_TABLE_NAME,BENCH_KEY_FIELD,BENCH_KEY_VALUE,BENCH_REGION_NAME,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN`
* `poetry run python -m pyperf compare <aiobotocore|aiodynamo_aiohttp|aiodynamo_httpx|boto3|botocore>.json  <aiobotocore|aiodynamo_aiohttp|aiodynamo_httpx|boto3|botocore>.json [...]`
