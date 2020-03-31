# Deserialize benchmark

Benchmarks DynamoDB datastructure to Python datastructure deserialization.

## Dependencies

You'll need `boto3`, `aiodynamo` and `pyperf` installed.

## Run

* `python bench_boto3.py -o boto3.json --rigorous`
* `python bench_aiodynamo.py -o aiodynamo.json --rigorous`

## Compare

`pyperf compare_to boto3.json aiodynamo.json`
