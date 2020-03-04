from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import *

from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from yarl import URL

from ..errors import *
from ..models import (
    GlobalSecondaryIndex,
    KeySchema,
    KeySpec,
    KeyType,
    LocalSecondaryIndex,
    ProjectionExpr,
    ReturnValues,
    Select,
    StreamSpecification,
    TableDescription,
    TableStatus,
    Throughput,
    TimeToLiveDescription,
    TimeToLiveStatus,
    UpdateExpression,
    get_projection,
)
from ..types import Item, TableName
from ..utils import dy2py, py2dy
from .credentials import Credentials
from .http.base import HTTP
from .sign import signed_dynamo_request


@dataclass(frozen=True)
class FastTimeToLive:
    table: FastTable

    async def enable(self, attribute: str):
        await self.table.client.enable_time_to_live(self.table.name, attribute)

    async def disable(self, attribute: str):
        await self.table.client.disable_time_to_live(self.table.name, attribute)

    async def describe(self) -> TimeToLiveDescription:
        return await self.table.client.describe_time_to_live(self.table.name)


@dataclass(frozen=True)
class FastTable:
    client: FastClient
    name: TableName

    async def exists(self) -> bool:
        return await self.client.table_exists(self.name)

    async def create(
        self,
        throughput: Throughput,
        keys: KeySchema,
        *,
        lsis: List[LocalSecondaryIndex] = None,
        gsis: List[GlobalSecondaryIndex] = None,
        stream: StreamSpecification = None,
    ):
        return await self.client.create_table(
            self.name, throughput, keys, lsis=lsis, gsis=gsis, stream=stream
        )

    @property
    def time_to_live(self):
        return FastTimeToLive(self)

    async def describe(self) -> TableDescription:
        return await self.client.describe_table(self.name)

    async def delete(self):
        return await self.client.delete_table(self.name)

    async def delete_item(
        self,
        key: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[None, Item]:
        return await self.client.delete_item(
            self.name, key, return_values=return_values, condition=condition
        )

    async def get_item(
        self, key: Dict[str, Any], *, projection: ProjectionExpr = None
    ) -> Item:
        return await self.client.get_item(self.name, key, projection=projection)

    async def put_item(
        self,
        item: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[None, Item]:
        return await self.client.put_item(
            self.name, item, return_values=return_values, condition=condition
        )

    def query(
        self,
        key_condition: ConditionBase,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: ConditionBase = None,
        scan_forward: bool = True,
        index: str = None,
        limit: int = None,
        projection: ProjectionExpr = None,
        select: Select = Select.all_attributes,
    ) -> AsyncIterator[Item]:
        return self.client.query(
            self.name,
            key_condition,
            start_key=start_key,
            filter_expression=filter_expression,
            scan_forward=scan_forward,
            index=index,
            limit=limit,
            projection=projection,
            select=select,
        )

    def scan(
        self,
        *,
        index: str = None,
        limit: int = None,
        start_key: Dict[str, Any] = None,
        projection: ProjectionExpr = None,
        filter_expression: ConditionBase = None,
    ) -> AsyncIterator[Item]:
        return self.client.scan(
            self.name,
            index=index,
            limit=limit,
            start_key=start_key,
            projection=projection,
            filter_expression=filter_expression,
        )

    async def count(
        self,
        key_condition: ConditionBase,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: ConditionBase = None,
        index: str = None,
    ) -> int:
        return await self.client.count(
            self.name,
            key_condition,
            start_key=start_key,
            filter_expression=filter_expression,
            index=index,
        )

    async def update_item(
        self,
        key: Item,
        update_expression: UpdateExpression,
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[Item, None]:
        return await self.client.update_item(
            self.name,
            key,
            update_expression,
            return_values=return_values,
            condition=condition,
        )


@dataclass(frozen=True)
class FastClient:
    http: HTTP
    credentials: Credentials
    region: str
    endpoint: Optional[URL] = None
    numeric_type: Callable[[Any], Any] = float

    async def table_exists(self, name: TableName) -> bool:
        try:
            description = await self.describe_table(name)
        except TableNotFound:
            return False

        return description.status is TableStatus.active

    async def create_table(
        self,
        name: TableName,
        throughput: Throughput,
        keys: KeySchema,
        *,
        lsis: List[LocalSecondaryIndex] = None,
        gsis: List[GlobalSecondaryIndex] = None,
        stream: StreamSpecification = None,
    ):
        attributes = keys.to_attributes()
        if lsis is not None:
            for index in lsis:
                attributes.update(index.schema.to_attributes())
        if gsis is not None:
            for index in gsis:
                attributes.update(index.schema.to_attributes())
        attribute_definitions = [
            {"AttributeName": key, "AttributeType": value}
            for key, value in attributes.items()
        ]
        payload = {
            "AttributeDefinitions": attribute_definitions,
            "TableName": name,
            "KeySchema": keys.encode(),
            "ProvisionedThroughput": throughput.encode(),
        }
        if lsis:
            payload["LocalSecondaryIndexes"] = [index.encode() for index in lsis]
        if gsis:
            payload["GlobalSecondaryIndexes"] = [index.encode() for index in gsis]
        if stream:
            payload["StreamSpecification"] = stream.encode()

        await self.send_request(action="CreateTable", payload=payload)

    async def enable_time_to_live(self, table: TableName, attribute: str):
        await self.set_time_to_live(table, attribute, True)

    async def describe_time_to_live(self, table: TableName) -> TimeToLiveDescription:
        response = await self.send_request(
            action="DescribeTimeToLive", payload={"TableName": table}
        )
        return TimeToLiveDescription(
            table=table,
            attribute=response["TimeToLiveDescription"].get("AttributeName"),
            status=TimeToLiveStatus(
                response["TimeToLiveDescription"]["TimeToLiveStatus"]
            ),
        )

    async def disable_time_to_live(self, table: TableName, attribute: str):
        await self.set_time_to_live(table, attribute, False)

    async def set_time_to_live(self, table: TableName, attribute: str, status: bool):
        await self.send_request(
            action="UpdateTimeToLive",
            payload={
                "TableName": table,
                "TimeToLiveSpecification": {
                    "AttributeName": attribute,
                    "Enabled": status,
                },
            },
        )

    async def describe_table(self, name: TableName):
        response = await self.send_request(
            action="DescribeTable", payload={"TableName": name}
        )

        description = response["Table"]
        attributes: Dict[str, KeyType] = {
            attribute["AttributeName"]: KeyType(attribute["AttributeType"])
            for attribute in description["AttributeDefinitions"]
        }
        return TableDescription(
            attributes=attributes,
            created=description["CreationDateTime"],
            item_count=description["ItemCount"],
            key_schema=KeySchema(
                *[
                    KeySpec(
                        name=key["AttributeName"], type=attributes[key["AttributeName"]]
                    )
                    for key in description["KeySchema"]
                ]
            ),
            throughput=Throughput(
                read=description["ProvisionedThroughput"]["ReadCapacityUnits"],
                write=description["ProvisionedThroughput"]["WriteCapacityUnits"],
            ),
            status=TableStatus(description["TableStatus"]),
        )

    async def delete_item(
        self,
        table: str,
        key: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[None, Item]:
        key = py2dy(key)
        if not key:
            raise EmptyItem()

        payload = {
            "TableName": table,
            "Key": key,
            "ReturnValues": return_values.value,
        }

        if condition:
            (
                payload["ConditionExpression"],
                payload["ExpressionAttributeNames"],
                payload["ExpressionAttributeValues"],
            ) = ConditionExpressionBuilder().build_expression(condition)

        resp = await self.send_request(action="DeleteItem", payload=payload)
        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def delete_table(self, table: TableName):
        await self.send_request(action="DeleteTable", payload={"TableName": table})

    async def get_item(
        self,
        table: TableName,
        key: Dict[str, Any],
        *,
        projection: ProjectionExpr = None,
    ) -> Item:
        projection_expression, expression_attribute_names = get_projection(projection)
        key = py2dy(key)
        if not key:
            raise EmptyItem()

        payload = {
            "TableName": table,
            "Key": key,
        }
        if projection_expression:
            payload["ProjectionExpression"] = projection_expression
            payload["ExpressionAttributeNames"] = expression_attribute_names

        resp = await self.send_request(action="GetItem", payload=payload)
        if "Item" in resp:
            return dy2py(resp["Item"], self.numeric_type)

        else:
            raise ItemNotFound(key)

    async def put_item(
        self,
        table: TableName,
        item: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[None, Item]:
        item = py2dy(item)
        if not item:
            raise EmptyItem()
        payload = {
            "TableName": table,
            "Item": item,
            "ReturnValues": return_values.value,
        }
        if condition:
            (
                payload["ConditionExpression"],
                payload["ExpressionAttributeNames"],
                eav,
            ) = ConditionExpressionBuilder().build_expression(condition)
            payload["ExpressionAttributeValues"] = py2dy(eav)

        resp = await self.send_request(action="PutItem", payload=payload)

        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def query(
        self,
        table: TableName,
        key_condition: ConditionBase,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: ConditionBase = None,
        scan_forward: bool = True,
        index: str = None,
        limit: int = None,
        projection: ProjectionExpr = None,
        select: Select = Select.all_attributes,
    ) -> AsyncIterator[Item]:
        if projection:
            select = Select.specific_attributes
        if select is Select.count:
            raise TypeError("Cannot use Select.count with query, use count instead")

        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = get_projection(projection)
        expression_attribute_names.update(ean)

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        key_condition_expression, ean, eav = builder.build_expression(
            key_condition, True
        )
        expression_attribute_names.update(ean)
        expression_attribute_values.update(eav)

        payload = {
            "TableName": table,
            "KeyConditionExpression": key_condition_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": py2dy(expression_attribute_values),
            "ScanForward": scan_forward,
        }
        if start_key:
            payload["ExclusiveStartKey"] = start_key
        if filter_expression:
            payload["FilterExpression"] = filter_expression
        if index:
            payload["IndexName"] = index
        if limit:
            payload["Limit"] = limit
        if projection_expression:
            payload["ProjectionExpression"] = projection_expression
        if select:
            payload["Select"] = select.value

        async for result in self._fast_depaginate("Query", payload):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def scan(
        self,
        table: TableName,
        *,
        index: str = None,
        limit: int = None,
        start_key: Dict[str, Any] = None,
        projection: ProjectionExpr = None,
        filter_expression: ConditionBase = None,
    ) -> AsyncIterator[Item]:
        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = get_projection(projection)
        expression_attribute_names.update(ean)

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        payload = {
            "TableName": table,
        }
        if index:
            payload["IndexName"] = index
        if limit:
            payload["Limit"] = limit
        if start_key:
            payload["ExclusiveStartKey"] = start_key
        if projection:
            payload["ProjectionExpression"] = projection_expression
        if filter_expression:
            payload["FilterExpression"] = filter_expression
        if projection or filter_expression:
            payload["ExpressionAttributeNames"] = expression_attribute_names
            payload["ExpressionAttributeValues"] = py2dy(expression_attribute_values)

        async for result in self._fast_depaginate("Scan", payload):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def count(
        self,
        table: TableName,
        key_condition: ConditionBase,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: ConditionBase = None,
        index: str = None,
    ) -> int:
        expression_attribute_names = {}
        expression_attribute_values = {}

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        key_condition_expression, ean, eav = builder.build_expression(
            key_condition, True
        )
        expression_attribute_names.update(ean)
        expression_attribute_values.update(eav)

        payload = {
            "TableName": table,
            "KeyConditionExpression": key_condition_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": py2dy(expression_attribute_values),
            "Select": Select.count.value,
        }
        if start_key:
            payload["ExclusiveStartKey"] = start_key
        if filter_expression:
            payload["FilterExpression"] = filter_expression
        if index:
            payload["IndexName"] = index
        count_sum = 0
        async for result in self._fast_depaginate("Query", payload):
            count_sum += result["Count"]
        return count_sum

    async def update_item(
        self,
        table: TableName,
        key: Item,
        update_expression: UpdateExpression,
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None,
    ) -> Union[Item, None]:

        (
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        ) = update_expression.encode()

        if not update_expression:
            raise EmptyItem()

        builder = ConditionExpressionBuilder()
        if condition:
            condition_expression, ean, eav = builder.build_expression(condition)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            condition_expression = None

        payload = {
            "TableName": table,
            "Key": py2dy(key),
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": py2dy(expression_attribute_values),
            "ReturnValues": return_values.value,
        }
        if condition:
            payload["ConditionExpression"] = condition_expression

        resp = await self.send_request(action="UpdateItem", payload=payload)

        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)
        else:
            return None

    async def send_request(
        self, *, action: str, payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        key = await self.credentials.get_key(self.http)
        request = signed_dynamo_request(
            key=key,
            payload=payload,
            action=action,
            region=self.region,
            endpoint=self.endpoint,
        )
        return await self.http.post(
            url=request.url, headers=request.headers, body=request.body
        )

    async def _fast_depaginate(self, action, payload) -> AsyncIterator[Dict[str, Any]]:
        task = asyncio.create_task(self.send_request(action=action, payload=payload))
        try:
            while task:
                result = await task
                try:
                    # TODO: Limit!
                    payload["ExclusiveStartKey"] = result["LastEvaluatedKey"]
                    task = asyncio.create_task(
                        self.send_request(action=action, payload=payload)
                    )
                except KeyError:
                    task = None
                yield result
        except asyncio.CancelledError:
            if task:
                task.cancel()
            raise
