from pyperf import Runner

from aiodynamo.sign import signed_dynamo_request
from data import *


def sign_aiodynamo():
    signed_dynamo_request(key=KEY, payload=PAYLOAD, action=ACTION, region=REGION, dual_stack=False)


Runner().bench_func("sign", sign_aiodynamo)
