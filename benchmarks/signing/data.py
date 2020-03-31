from botocore.credentials import Credentials

from aiodynamo.credentials import Key
from aiodynamo.sign import make_default_endpoint

KEY = Key("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
CREDENTIALS = Credentials(KEY.id, KEY.secret)
SERVICE_NAME = "dynamodb"
REGION = "us-east-1"
URL = make_default_endpoint(REGION)
URL_STRING = str(URL)
ACTION = "Query"
PAYLOAD = {
    "TableName": "table-name",
    "KeyConditionExpression": "#n0.#n1.#n2.#n3.#n4.#n5.#n3.#n2.#n6.#n7.#n1 = :v0 AND size(#n8) <= :v1",
    "ScanIndexForward": True,
    "ProjectionExpression": "#n9,#n1,#n10[4].#n11",
    "FilterExpression": "(begins_with(#n12, :v2) AND contains(#n13, :v3))",
    "ExclusiveStartKey": {
        "my-hash-key": {"S": "some-value"},
        "my-range-key": {"S": "other-value"},
    },
    "Select": "SPECIFIC_ATTRIBUTES",
    "ExpressionAttributeNames": {
        "#n0": "m",
        "#n1": "y",
        "#n2": "-",
        "#n3": "h",
        "#n4": "a",
        "#n5": "s",
        "#n6": "k",
        "#n7": "e",
        "#n8": "my-range-key",
        "#n9": "x",
        "#n10": "z",
        "#n11": "alpha",
        "#n12": "foo",
        "#n13": "hoge",
    },
    "ExpressionAttributeValues": {
        ":v0": {"S": "some-value"},
        ":v1": {"N": "200"},
        ":v2": {"S": "bar"},
        ":v3": {"B": "aGVsbG8gd29ybGQ="},
    },
}
