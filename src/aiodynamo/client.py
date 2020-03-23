from __future__ import annotations

import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import *

from yarl import URL

from .credentials import Credentials
from .errors import *
from .errors import (
    TableDidNotBecomeActive,
    TableDidNotBecomeDisabled,
    Throttled,
    TimeToLiveStatusNotChanged,
)
from .expressions import (
    Condition,
    KeyCondition,
    Parameters,
    ProjectionExpression,
    UpdateExpression,
)
from .http.base import HTTP
from .models import (
    GlobalSecondaryIndex,
    KeySchema,
    KeySpec,
    KeyType,
    LocalSecondaryIndex,
    ReturnValues,
    Select,
    StreamSpecification,
    TableDescription,
    TableStatus,
    ThrottleConfig,
    Throughput,
    TimeToLiveDescription,
    TimeToLiveStatus,
    WaitConfig,
)
from .sign import signed_dynamo_request
from .types import Item, TableName
from .utils import dy2py, py2dy


@dataclass(frozen=True)
class TimeToLive:
    table: Table

    async def enable(
        self, attribute: str, *, wait_for_enabled: Union[bool, WaitConfig] = False
    ):
        await self.table.client.enable_time_to_live(
            self.table.name, attribute, wait_for_enabled=wait_for_enabled
        )

    async def disable(
        self, attribute: str, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ):
        await self.table.client.disable_time_to_live(
            self.table.name, attribute, wait_for_disabled=wait_for_disabled
        )

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
        wait_for_active: Union[bool, WaitConfig] = False,
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
        condition: Condition = None,
    ) -> Union[None, Item]:
        return await self.client.delete_item(
            self.name, key, return_values=return_values, condition=condition
        )

    async def get_item(
        self, key: Dict[str, Any], *, projection: ProjectionExpression = None
    ) -> Item:
        return await self.client.get_item(self.name, key, projection=projection)

    async def put_item(
        self,
        item: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: Optional[Condition] = None,
    ) -> Union[None, Item]:
        return await self.client.put_item(
            self.name, item, return_values=return_values, condition=condition
        )

    def query(
        self,
        key_condition: KeyCondition,
        *,
        start_key: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Condition] = None,
        scan_forward: bool = True,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        projection: Optional[ProjectionExpression] = None,
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
        projection: ProjectionExpression = None,
        filter_expression: Condition = None,
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
        key_condition: KeyCondition,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: Condition = None,
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
        condition: Condition = None,
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
    http: HTTP
    credentials: Credentials
    region: str
    endpoint: Optional[URL] = None
    numeric_type: Callable[[Any], Any] = float
    throttle_config: ThrottleConfig = ThrottleConfig.default()

    def table(self, name: str) -> Table:
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
        wait_for_active: Union[bool, WaitConfig] = False,
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
        if wait_for_active:
            if not isinstance(wait_for_active, WaitConfig):
                wait_for_active = WaitConfig.default()
            async for _ in wait_for_active.attempts():
                try:
                    description = await self.describe_table(name)
                    if description.status == TableStatus.active:
                        return
                except TableNotFound:
                    pass
            raise TableDidNotBecomeActive()

    async def enable_time_to_live(
        self,
        table: TableName,
        attribute: str,
        *,
        wait_for_enabled: Union[bool, WaitConfig] = False,
    ):
        await self.set_time_to_live(
            table, attribute, True, wait_for_change=wait_for_enabled
        )

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

    async def disable_time_to_live(
        self,
        table: TableName,
        attribute: str,
        *,
        wait_for_disabled: Union[bool, WaitConfig] = False,
    ):
        await self.set_time_to_live(
            table, attribute, False, wait_for_change=wait_for_disabled
        )

    async def set_time_to_live(
        self,
        table: TableName,
        attribute: str,
        status: bool,
        *,
        wait_for_change: Union[bool, WaitConfig] = False,
    ):
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
        if wait_for_change:
            result_state = (
                TimeToLiveStatus.enabled if status else TimeToLiveStatus.disabled
            )
            if not isinstance(wait_for_change, WaitConfig):
                wait_for_change = WaitConfig.default()
            async for _ in wait_for_change.attempts():
                description = await self.describe_time_to_live(table)
                if description.status == result_state:
                    return
            raise TimeToLiveStatusNotChanged()

    async def describe_table(self, name: TableName):
        response = await self.send_request(
            action="DescribeTable", payload={"TableName": name}
        )

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
        condition: Condition = None,
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
            params = Parameters()
            payload["ConditionExpression"] = condition.encode(params)
            payload["ExpressionAttributeNames"] = params.get_expression_names()
            payload["ExpressionAttributeValues"] = params.get_expression_values()

        resp = await self.send_request(action="DeleteItem", payload=payload)
        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def delete_table(
        self, table: TableName, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ):
        await self.send_request(action="DeleteTable", payload={"TableName": table})
        if not isinstance(wait_for_disabled, WaitConfig):
            wait_for_disabled = WaitConfig.default()
        async for _ in wait_for_disabled.attempts():
            try:
                await self.describe_table(table)
            except TableNotFound:
                return
        raise TableDidNotBecomeDisabled()

    async def get_item(
        self,
        table: TableName,
        key: Dict[str, Any],
        *,
        projection: ProjectionExpression = None,
    ) -> Item:
        key = py2dy(key)
        if not key:
            raise EmptyItem()

        payload = {
            "TableName": table,
            "Key": key,
        }
        if projection:
            params = Parameters()
            payload["ProjectionExpression"] = projection.encode(params)
            payload["ExpressionAttributeNames"] = params.get_expression_names()

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
        condition: Condition = None,
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
            params = Parameters()
            payload["ConditionExpression"] = condition.encode(params)
            payload["ExpressionAttributeNames"] = params.get_expression_names()
            payload["ExpressionAttributeValues"] = params.get_expression_values()

        resp = await self.send_request(action="PutItem", payload=payload)

        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def query(
        self,
        table: TableName,
        key_condition: KeyCondition,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: Condition = None,
        scan_forward: bool = True,
        index: str = None,
        limit: int = None,
        projection: ProjectionExpression = None,
        select: Select = Select.all_attributes,
    ) -> AsyncIterator[Item]:
        if projection:
            select = Select.specific_attributes
        if select is Select.count:
            raise TypeError("Cannot use Select.count with query, use count instead")

        params = Parameters()

        payload = {
            "TableName": table,
            "KeyConditionExpression": key_condition.encode(params),
            "ScanIndexForward": scan_forward,
        }

        if projection:
            payload["ProjectionExpression"] = projection.encode(params)

        if filter_expression:
            payload["FilterExpression"] = filter_expression.encode(params)

        if start_key:
            payload["ExclusiveStartKey"] = py2dy(start_key)
        if index:
            payload["IndexName"] = index
        if select:
            payload["Select"] = select.value

        payload["ExpressionAttributeNames"] = params.get_expression_names()
        payload["ExpressionAttributeValues"] = params.get_expression_values()

        async for result in self._depaginate("Query", payload, limit):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def scan(
        self,
        table: TableName,
        *,
        index: str = None,
        limit: int = None,
        start_key: Dict[str, Any] = None,
        projection: ProjectionExpression = None,
        filter_expression: Condition = None,
    ) -> AsyncIterator[Item]:

        params = Parameters()

        payload = {
            "TableName": table,
        }

        if index:
            payload["IndexName"] = index
        if start_key:
            payload["ExclusiveStartKey"] = py2dy(start_key)
        if projection:
            payload["ProjectionExpression"] = projection.encode(params)
        if filter_expression:
            payload["FilterExpression"] = filter_expression.encode(params)
        if projection or filter_expression:
            payload["ExpressionAttributeNames"] = params.get_expression_names()
            payload["ExpressionAttributeValues"] = params.get_expression_values()

        async for result in self._depaginate("Scan", payload, limit):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def count(
        self,
        table: TableName,
        key_condition: KeyCondition,
        *,
        start_key: Dict[str, Any] = None,
        filter_expression: Condition = None,
        index: str = None,
    ) -> int:
        params = Parameters()

        payload = {
            "TableName": table,
            "KeyConditionExpression": key_condition.encode(params),
            "Select": Select.count.value,
        }

        if start_key:
            payload["ExclusiveStartKey"] = py2dy(start_key)
        if filter_expression:
            payload["FilterExpression"] = filter_expression.encode(params)
        if index:
            payload["IndexName"] = index
        payload["ExpressionAttributeNames"] = params.get_expression_names()
        payload["ExpressionAttributeValues"] = params.get_expression_values()
        count_sum = 0
        async for result in self._depaginate("Query", payload):
            count_sum += result["Count"]
        return count_sum

    async def update_item(
        self,
        table: TableName,
        key: Item,
        update_expression: UpdateExpression,
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: Condition = None,
    ) -> Union[Item, None]:
        params = Parameters()

        expression = update_expression.encode(params)
        if not expression:
            raise EmptyItem()

        payload = {
            "TableName": table,
            "Key": py2dy(key),
            "UpdateExpression": expression,
            "ReturnValues": return_values.value,
        }
        if condition:
            payload["ConditionExpression"] = condition.encode(params)

        payload["ExpressionAttributeNames"] = params.get_expression_names()
        values = params.get_expression_values()
        if values:
            payload["ExpressionAttributeValues"] = values

        resp = await self.send_request(action="UpdateItem", payload=payload)

        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)
        else:
            return None

    async def send_request(
        self, *, action: str, payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        async for _ in self.throttle_config.attempts():
            key = await self.credentials.get_key(self.http)
            request = signed_dynamo_request(
                key=key,
                payload=payload,
                action=action,
                region=self.region,
                endpoint=self.endpoint,
            )
            try:
                logging.debug("sending request %r", request)
                return await self.http.post(
                    url=request.url, headers=request.headers, body=request.body
                )
            except Throttled:
                logging.debug("request throttled")
            except ProvisionedThroughputExceeded:
                logging.debug("provisioned throughput exceeded")
            except ExpiredToken:
                logging.debug("token expired")
                if not self.credentials.invalidate():
                    raise

    async def _depaginate(
        self, action: str, payload: Dict[str, Any], limit: Optional[int] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Internal API to depaginate the results from query/scan/count.
        Don't call this directly, use .query, .scan or .count instead.
        """
        if limit is not None:
            payload = {**payload, "Limit": limit}
        task = asyncio.create_task(self.send_request(action=action, payload=payload))
        try:
            while task:
                result = await task
                try:
                    payload = {
                        **payload,
                        "ExclusiveStartKey": result["LastEvaluatedKey"],
                    }
                except KeyError:
                    payload = None
                else:
                    if limit is not None:
                        limit -= len(result["Items"])
                        if limit > 0:
                            payload["Limit"] = limit
                        else:
                            payload = None
                if payload:
                    task = asyncio.create_task(
                        self.send_request(action=action, payload=payload)
                    )
                else:
                    task = None
                yield result
        except asyncio.CancelledError:
            if task:
                task.cancel()
            raise
