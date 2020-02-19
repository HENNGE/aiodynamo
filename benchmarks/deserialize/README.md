Run:

* `poetry run  python bench_boto3.py -o boto3.json --rigorous`
* `poetry run python bench_aiodynamo.py -o aiodynamo.json --rigorous`
* `poetry run python -m pyperf compare boto3.json aiodynamo.json`
