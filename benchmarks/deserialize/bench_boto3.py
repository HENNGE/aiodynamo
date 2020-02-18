from boto3.dynamodb.types import TypeDeserializer
from pyperf import Runner

from utils import generate_data


data = generate_data()


def deserialize_aiodynamo():
    result = [
        {k: TypeDeserializer().deserialize(v) for k, v in item.items()} for item in data
    ]


Runner().bench_func("deserialize", deserialize_aiodynamo)
