import boto3
from boto3.dynamodb.conditions import Key
from pyperf import Runner

from utils import TABLE_NAME, KEY_FIELD, KEY_VALUE, REGION_NAME


def query_boto3():
    dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)

    items = []
    lek = None
    while True:
        if lek is None:
            kwargs = {}
        else:
            kwargs = {"ExclusiveStartKey": lek}
        response = table.query(
            KeyConditionExpression=Key(KEY_FIELD).eq(KEY_VALUE), **kwargs
        )
        items.extend(response["Items"])
        lek = response.get("LastEvaluatedKey", None)
        if lek is None:
            break


Runner().bench_func("query", query_boto3)
