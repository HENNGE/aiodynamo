from pyperf import Runner

from aiodynamo.utils import deserialize
from utils import generate_data


data = generate_data()


def deserialize_aiodynamo():
    result = [{k: deserialize(v, float) for k, v in item.items()} for item in data]


Runner().bench_func("deserialize", deserialize_aiodynamo)
