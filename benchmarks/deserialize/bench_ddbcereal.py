from pyperf import Runner

import ddbcereal
from utils import generate_data


data = generate_data()

deserialize_item = ddbcereal.Deserializer(
    allow_inexact=True,
    number_type=ddbcereal.PythonNumber.FLOAT_ONLY,
    raw_transport=True
).deserialize_item


def deserialize_aiodynamo():
    result = [deserialize_item(item) for item in data]


Runner().bench_func("deserialize", deserialize_aiodynamo)
