# Signing benchmark

Benchmarks DynamoDB request signing.

## Dependencies

You'll need `botocre`, `aiodynamo` and `pyperf` installed.

## Run

* `python bench_botocore.py -o botocore.json --rigorous`
* `python bench_aiodynamo.py -o aiodynamo.json --rigorous`

## Compare

`pyperf compare_to botocore.json aiodynamo.json`
