import json

from botocore.awsrequest import AWSRequest
from botocore.hooks import HierarchicalEmitter
from botocore.model import ServiceId
from botocore.signers import RequestSigner
from pyperf import Runner

from data import *


def sign_botocore():
    request = AWSRequest(
        "POST",
        URL_STRING,
        data=json.dumps(PAYLOAD, separators=(",", ":")).encode("utf-8"),
    )
    emitter = HierarchicalEmitter()
    RequestSigner(
        ServiceId(SERVICE_NAME), REGION, SERVICE_NAME, "v4", CREDENTIALS, emitter
    ).sign(ACTION, request)


Runner().bench_func("sign", sign_botocore)
