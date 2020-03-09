import asyncio

from aiobotocore import get_session
from pyperf import Runner

from utils import TABLE_NAME, KEY_FIELD, KEY_VALUE, REGION_NAME


async def inner():
    client = get_session().create_client("dynamodb", region_name=REGION_NAME)
    items = []
    lek = None
    while True:
        if lek is None:
            kwargs = {}
        else:
            kwargs = {"ExclusiveStartKey": lek}
        response = await client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#k = :v",
            ExpressionAttributeNames={"#k": KEY_FIELD},
            ExpressionAttributeValues={":v", {"S": KEY_VALUE}},
            **kwargs
        )
        items.extend(response["Items"])
        lek = response.get("LastEvaluatedKey", None)
        if lek is None:
            break


def query_aiobotocore():
    asyncio.run(inner())


Runner().bench_func("query", query_aiobotocore)
