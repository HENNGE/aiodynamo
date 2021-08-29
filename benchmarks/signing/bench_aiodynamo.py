from data import *
from pyperf import Runner

from aiodynamo.client import dumps
from aiodynamo.sign import signed_dynamo_request


def sign_aiodynamo():
    signed_dynamo_request(
        key=KEY, payload=PAYLOAD, action=ACTION, region=REGION, dumps=dumps
    )


Runner().bench_func("sign", sign_aiodynamo)
