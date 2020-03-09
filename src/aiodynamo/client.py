from __future__ import annotations

import asyncio
import datetime
from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import Any, AsyncIterator, Callable, Dict, List, TypeVar, Union

from aiobotocore.client import AioBaseClient
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from boto3.dynamodb.types import DYNAMODB_CONTEXT
from botocore.exceptions import ClientError

from .errors import EmptyItem, ItemNotFound, TableNotFound
from .fast.errors import TableDidNotBecomeActive, TableDidNotBecomeDisabled
from .models import (
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
    WaitConfig,
    get_projection,
)
from .types import Item, TableName
from .utils import clean, dy2py, py2dy, unroll

_Key = TypeVar("_Key")
_Val = TypeVar("_Val")


@dataclass(frozen=True)
class TimeToLive:
    table: Table

    async def enable(self, attribute: str):
        await self.table.client.enable_time_to_live(self.table.name, attribute)

    async def disable(self, attribute: str):
        await self.table.client.disable_time_to_live(self.table.name, attribute)

    async def describe(self) -> TimeToLiveDescription:
        return await self.table.client.describe_time_to_live(self.table.name)


@dataclass(frozen=True)
class Table:
    client: Client
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
        wait_for_active: Union[bool, WaitConfig] = False
    ):
        return await self.client.create_table(
            self.name,
            throughput,
            keys,
            lsis=lsis,
            gsis=gsis,
            stream=stream,
            wait_for_active=wait_for_active,
        )

    @property
    def time_to_live(self):
        return TimeToLive(self)

    async def describe(self) -> TableDescription:
        return await self.client.describe_table(self.name)

    async def delete(self, *, wait_for_disabled: Union[bool, WaitConfig] = False):
        return await self.client.delete_table(
            self.name, wait_for_disabled=wait_for_disabled
        )

    async def delete_item(
        self,
        key: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None
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
        condition: ConditionBase = None
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
        select: Select = Select.all_attributes
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
        filter_expression: ConditionBase = None
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
        index: str = None
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
        condition: ConditionBase = None
    ) -> Union[Item, None]:
        return await self.client.update_item(
            self.name,
            key,
            update_expression,
            return_values=return_values,
            condition=condition,
        )


@dataclass(frozen=True)
class Client:
    # core is an aiobotocore DynamoDB client, use aiobotocore.get_session().create_client("dynamodb") to create one.
    core: AioBaseClient
    # pass `float` if you want numeric types returned as floats
    numeric_type: Callable[[str], Any] = DYNAMODB_CONTEXT.create_decimal

    def table(self, name: TableName) -> Table:
        return Table(self, name)

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
        wait_for_active: Union[bool, WaitConfig] = False
    ):
        lsis: List[LocalSecondaryIndex] = lsis or []
        gsis: List[GlobalSecondaryIndex] = gsis or []
        stream = stream or StreamSpecification()
        attributes = {}
        attributes.update(keys.to_attributes())
        for index in chain(lsis, gsis):
            attributes.update(index.schema.to_attributes())
        attribute_definitions = [
            {"AttributeName": key, "AttributeType": value}
            for key, value in attributes.items()
        ]
        key_schema = keys.encode()
        local_secondary_indexes = [index.encode() for index in lsis]
        global_secondary_indexes = [index.encode() for index in gsis]
        provisioned_throughput = throughput.encode()
        stream_specification = stream.encode()
        await self.core.create_table(
            **clean(
                AttributeDefinitions=attribute_definitions,
                TableName=name,
                KeySchema=key_schema,
                LocalSecondaryIndexes=local_secondary_indexes,
                GlobalSecondaryIndexes=global_secondary_indexes,
                ProvisionedThroughput=provisioned_throughput,
                StreamSpecification=stream_specification,
            )
        )
        if wait_for_active:
            if not isinstance(wait_for_active, WaitConfig):
                wait_for_active = WaitConfig.default()
            attempts = 0
            while attempts < wait_for_active.max_attempts:
                if await self.table_exists(name):
                    return
                attempts += 1
                await asyncio.sleep(wait_for_active.retry_delay)
            raise TableDidNotBecomeActive()
        return None

    async def enable_time_to_live(self, table: TableName, attribute: str):
        await self.core.update_time_to_live(
            TableName=table,
            TimeToLiveSpecification={"AttributeName": attribute, "Enabled": True},
        )

    async def describe_time_to_live(self, table: TableName) -> TimeToLiveDescription:
        response = await self.core.describe_time_to_live(TableName=table)
        return TimeToLiveDescription(
            table=table,
            attribute=response["TimeToLiveDescription"].get("AttributeName"),
            status=TimeToLiveStatus(
                response["TimeToLiveDescription"]["TimeToLiveStatus"]
            ),
        )

    async def disable_time_to_live(self, table: TableName, attribute: str):
        await self.core.update_time_to_live(
            TableName=table,
            TimeToLiveSpecification={"AttributeName": attribute, "Enabled": False},
        )

    async def describe_table(self, name: TableName):
        try:
            response = await self.core.describe_table(TableName=name)
        except ClientError as exc:
            try:
                if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise TableNotFound(name)

            except KeyError:
                pass
            raise exc

        description = response["Table"]
        if "AttributeDefinitions" in description:
            attributes = {
                attribute["AttributeName"]: KeyType(attribute["AttributeType"])
                for attribute in description["AttributeDefinitions"]
            }
        else:
            attributes = None
        if "CreationDateTime" in description:
            creation_time = datetime.datetime.fromtimestamp(
                description["CreationDateTime"], datetime.timezone.utc
            )
        else:
            creation_time = None
        if attributes and "KeySchema" in description:
            key_schema = KeySchema(
                *[
                    KeySpec(
                        name=key["AttributeName"], type=attributes[key["AttributeName"]]
                    )
                    for key in description["KeySchema"]
                ]
            )
        else:
            key_schema = None
        if "ProvisionedThroughput" in description:
            throughput = Throughput(
                read=description["ProvisionedThroughput"]["ReadCapacityUnits"],
                write=description["ProvisionedThroughput"]["WriteCapacityUnits"],
            )
        else:
            throughput = None
        return TableDescription(
            attributes=attributes,
            created=creation_time,
            item_count=description.get("ItemCount", None),
            key_schema=key_schema,
            throughput=throughput,
            status=TableStatus(description["TableStatus"]),
        )

    async def delete_item(
        self,
        table: str,
        key: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None
    ) -> Union[None, Item]:
        key = py2dy(key)
        if not key:
            raise EmptyItem()

        if condition:
            (
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            ) = ConditionExpressionBuilder().build_expression(condition)
        else:
            condition_expression = (
                expression_attribute_names
            ) = expression_attribute_values = None
        resp = await self.core.delete_item(
            **clean(
                TableName=table,
                Key=key,
                ReturnValues=return_values.value,
                ConditionExpression=condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
        )
        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def delete_table(
        self, table: TableName, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ):
        await self.core.delete_table(TableName=table)
        if not isinstance(wait_for_disabled, WaitConfig):
            wait_for_disabled = WaitConfig.default()
        attempts = 0
        while attempts < wait_for_disabled.max_attempts:
            try:
                await self.describe_table(table)
            except TableNotFound:
                return
            attempts += 1
            await asyncio.sleep(wait_for_disabled.retry_delay)
        raise TableDidNotBecomeDisabled()

    async def get_item(
        self,
        table: TableName,
        key: Dict[str, Any],
        *,
        projection: ProjectionExpr = None
    ) -> Item:
        projection_expression, expression_attribute_names = get_projection(projection)
        key = py2dy(key)
        if not key:
            raise EmptyItem()

        resp = await self.core.get_item(
            **clean(
                TableName=table,
                Key=key,
                ProjectionExpression=projection_expression,
                ExpressionAttributeNames=expression_attribute_names,
            )
        )
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
        condition: ConditionBase = None
    ) -> Union[None, Item]:
        if condition:
            (
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            ) = ConditionExpressionBuilder().build_expression(condition)
        else:
            condition_expression = (
                expression_attribute_names
            ) = expression_attribute_values = None
        item = py2dy(item)
        if not item:
            raise EmptyItem()

        resp = await self.core.put_item(
            **clean(
                TableName=table,
                Item=item,
                ReturnValues=return_values.value,
                ConditionExpression=condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=py2dy(expression_attribute_values),
            )
        )
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
        select: Select = Select.all_attributes
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

        if key_condition:
            key_condition_expression, ean, eav = builder.build_expression(
                key_condition, True
            )
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            key_condition_expression = None

        coro_func = partial(
            self.core.query,
            **clean(
                TableName=table,
                IndexName=index,
                ScanIndexForward=scan_forward,
                ProjectionExpression=projection_expression,
                FilterExpression=filter_expression,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=py2dy(expression_attribute_values),
                Select=select.value,
            )
        )
        async for raw in unroll(
            coro_func,
            "ExclusiveStartKey",
            "LastEvaluatedKey",
            "Items",
            py2dy(start_key) if start_key else None,
            limit,
            "Limit",
        ):
            yield dy2py(raw, self.numeric_type)

    async def scan(
        self,
        table: TableName,
        *,
        index: str = None,
        limit: int = None,
        start_key: Dict[str, Any] = None,
        projection: ProjectionExpr = None,
        filter_expression: ConditionBase = None
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

        coro_func = partial(
            self.core.scan,
            **clean(
                TableName=table,
                IndexName=index,
                ProjectionExpression=projection_expression,
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=py2dy(expression_attribute_values),
            )
        )
        async for raw in unroll(
            coro_func,
            "ExclusiveStartKey",
            "LastEvaluatedKey",
            "Items",
            py2dy(start_key) if start_key else None,
            limit,
            "Limit",
        ):
            yield dy2py(raw, self.numeric_type)

    async def count(
        self,
        table: TableName,
        key_condition: ConditionBase,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: ConditionBase = None,
        index: str = None
    ) -> int:
        expression_attribute_names = {}
        expression_attribute_values = {}

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        if key_condition:
            key_condition_expression, ean, eav = builder.build_expression(
                key_condition, True
            )
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            key_condition_expression = None

        coro_func = partial(
            self.core.query,
            **clean(
                TableName=table,
                IndexName=index,
                FilterExpression=filter_expression,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=py2dy(expression_attribute_values),
                Select=Select.count.value,
            )
        )
        count_sum = 0
        async for count in unroll(
            coro_func,
            "ExclusiveStartKey",
            "LastEvaluatedKey",
            "Count",
            py2dy(start_key) if start_key else None,
            process=lambda x: [x],
        ):
            count_sum += count
        return count_sum

    async def update_item(
        self,
        table: TableName,
        key: Item,
        update_expression: UpdateExpression,
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: ConditionBase = None
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
        resp = await self.core.update_item(
            **clean(
                TableName=table,
                Key=py2dy(key),
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=py2dy(expression_attribute_values),
                ConditionExpression=condition_expression,
                ReturnValues=return_values.value,
            )
        )
        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None
