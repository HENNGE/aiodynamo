from __future__ import annotations

import asyncio
import datetime
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
from .http.base import HTTP, RequestFailed
from .models import (
    BatchGetRequest,
    BatchGetResponse,
    BatchWriteRequest,
    BatchWriteResult,
    GlobalSecondaryIndex,
    KeySchema,
    KeySpec,
    KeyType,
    LocalSecondaryIndex,
    Page,
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
from .types import Item, NumericTypeConverter, TableName
from .utils import dy2py, logger, py2dy


@dataclass(frozen=True)
class TimeToLive:
    table: Table

    async def enable(
        self, attribute: str, *, wait_for_enabled: Union[bool, WaitConfig] = False
    ) -> None:
        await self.table.client.enable_time_to_live(
            self.table.name, attribute, wait_for_enabled=wait_for_enabled
        )

    async def disable(
        self, attribute: str, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ) -> None:
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
        lsis: Optional[List[LocalSecondaryIndex]] = None,
        gsis: Optional[List[GlobalSecondaryIndex]] = None,
        stream: Optional[StreamSpecification] = None,
        wait_for_active: Union[bool, WaitConfig] = False,
    ) -> None:
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
    def time_to_live(self) -> TimeToLive:
        return TimeToLive(self)

    async def describe(self) -> TableDescription:
        return await self.client.describe_table(self.name)

    async def delete(
        self, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ) -> None:
        return await self.client.delete_table(
            self.name, wait_for_disabled=wait_for_disabled
        )

    async def delete_item(
        self,
        key: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: Optional[Condition] = None,
    ) -> Union[None, Item]:
        return await self.client.delete_item(
            self.name, key, return_values=return_values, condition=condition
        )

    async def get_item(
        self, key: Dict[str, Any], *, projection: Optional[ProjectionExpression] = None
    ) -> Item:
        """
        Returns the attributes of an item from table.
        This will return all attributes by default.
        To get only some attributes, use a projection expression.
        """
        return await self.client.get_item(self.name, key, projection=projection)

    async def put_item(
        self,
        item: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: Optional[Condition] = None,
    ) -> Union[None, Item]:
        """
        Create a new item or replace it if it already exists.
        This will overwrite all attributes in an item.
        """
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
        """
        Query the table.

        Unlike the DynamoDB API, the results are automatically de-paginated
        and a single stream of items is returned. For manual pagination, use
        query_single_page(...) instead.

        To filter the result, use a filter expression.
        This will return all attributes by default.
        To get only some attributes, use a projection expression.
        """
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

    async def query_single_page(
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
    ) -> Page:
        """
        Query a single DynamoDB page.
        To automatically handle pagination, uses query(...) instead.
        """
        return await self.client.query_single_page(
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
        index: Optional[str] = None,
        limit: Optional[int] = None,
        start_key: Optional[Dict[str, Any]] = None,
        projection: Optional[ProjectionExpression] = None,
        filter_expression: Optional[Condition] = None,
    ) -> AsyncIterator[Item]:
        """
        Scan the table.

        Unlike the DynamoDB API, the results are automatically de-paginated
        and a single stream of items is returned. For manual pagination, use
        scan_single_page(...) instead.

        To filter the result, use a filter expression.
        This will return all attributes by default.
        To get only some attributes, use a projection expression.
        """
        return self.client.scan(
            self.name,
            index=index,
            limit=limit,
            start_key=start_key,
            projection=projection,
            filter_expression=filter_expression,
        )

    async def scan_single_page(
        self,
        *,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        start_key: Optional[Dict[str, Any]] = None,
        projection: Optional[ProjectionExpression] = None,
        filter_expression: Optional[Condition] = None,
    ) -> Page:
        """
        Scan a single DynamoDB page.
        To automatically handle pagination, uses scan(...) instead.
        """
        return await self.client.scan_single_page(
            table=self.name,
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
        start_key: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Condition] = None,
        index: Optional[str] = None,
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
        condition: Optional[Condition] = None,
    ) -> Union[Item, None]:
        """
        Edit an item's attribute or create a new item if it does not exist.
        This will edit only the passed attributes.
        """
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
    numeric_type: NumericTypeConverter = float
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
        lsis: Optional[List[LocalSecondaryIndex]] = None,
        gsis: Optional[List[GlobalSecondaryIndex]] = None,
        stream: Optional[StreamSpecification] = None,
        wait_for_active: Union[bool, WaitConfig] = False,
    ) -> None:
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
            wait_config = (
                wait_for_active
                if isinstance(wait_for_active, WaitConfig)
                else WaitConfig.default()
            )
            async for _ in wait_config.attempts():
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
    ) -> None:
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
    ) -> None:
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
    ) -> None:
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
            wait_config = (
                wait_for_change
                if isinstance(wait_for_change, WaitConfig)
                else WaitConfig.default()
            )
            async for _ in wait_config.attempts():
                description = await self.describe_time_to_live(table)
                if description.status == result_state:
                    return
            raise TimeToLiveStatusNotChanged()

    async def describe_table(self, name: TableName) -> TableDescription:
        response = await self.send_request(
            action="DescribeTable", payload={"TableName": name}
        )

        description = response["Table"]
        attributes: Optional[Dict[str, KeyType]]
        if "AttributeDefinitions" in description:
            attributes = {
                attribute["AttributeName"]: KeyType(attribute["AttributeType"])
                for attribute in description["AttributeDefinitions"]
            }
        else:
            attributes = None
        creation_time: Optional[datetime.datetime]
        if "CreationDateTime" in description:
            creation_time = datetime.datetime.fromtimestamp(
                description["CreationDateTime"], datetime.timezone.utc
            )
        else:
            creation_time = None
        key_schema: Optional[KeySchema]
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
        throughput: Optional[Throughput]
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
        condition: Optional[Condition] = None,
    ) -> Union[None, Item]:
        dynamo_key = py2dy(key)
        if not dynamo_key:
            raise EmptyItem()

        payload: Dict[str, Any] = {
            "TableName": table,
            "Key": dynamo_key,
            "ReturnValues": return_values.value,
        }

        if condition:
            params = Parameters()
            payload["ConditionExpression"] = condition.encode(params)
            payload.update(params.to_request_payload())

        resp = await self.send_request(action="DeleteItem", payload=payload)
        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)

        else:
            return None

    async def delete_table(
        self, table: TableName, *, wait_for_disabled: Union[bool, WaitConfig] = False
    ) -> None:
        await self.send_request(action="DeleteTable", payload={"TableName": table})
        if wait_for_disabled:
            wait_config = (
                wait_for_disabled
                if isinstance(wait_for_disabled, WaitConfig)
                else WaitConfig.default()
            )
            async for _ in wait_config.attempts():
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
        projection: Optional[ProjectionExpression] = None,
    ) -> Item:
        dynamo_key = py2dy(key)
        if not dynamo_key:
            raise EmptyItem()

        payload: Dict[str, Any] = {
            "TableName": table,
            "Key": dynamo_key,
        }
        if projection:
            params = Parameters()
            payload["ProjectionExpression"] = projection.encode(params)
            payload.update(params.to_request_payload())

        resp = await self.send_request(action="GetItem", payload=payload)
        if "Item" in resp:
            return dy2py(resp["Item"], self.numeric_type)

        else:
            raise ItemNotFound(dynamo_key)

    async def put_item(
        self,
        table: TableName,
        item: Dict[str, Any],
        *,
        return_values: ReturnValues = ReturnValues.none,
        condition: Optional[Condition] = None,
    ) -> Union[None, Item]:
        dynamo_item = py2dy(item)
        if not dynamo_item:
            raise EmptyItem()
        payload: Dict[str, Any] = {
            "TableName": table,
            "Item": dynamo_item,
            "ReturnValues": return_values.value,
        }
        if condition:
            params = Parameters()
            payload["ConditionExpression"] = condition.encode(params)
            payload.update(params.to_request_payload())

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
        start_key: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Condition] = None,
        scan_forward: bool = True,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        projection: Optional[ProjectionExpression] = None,
        select: Select = Select.all_attributes,
    ) -> AsyncIterator[Item]:
        """
        Query the table.
        Unlike the DynamoDB API, the results are automatically de-paginated
        and a single stream of items is returned. For manual pagination, use
        query_single_page(...) instead.
        """
        payload = _query_payload(
            table=table,
            key_condition=key_condition,
            start_key=start_key,
            filter_expression=filter_expression,
            scan_forward=scan_forward,
            index=index,
            projection=projection,
            select=select,
        )

        async for result in self._depaginate("Query", payload, limit):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def query_single_page(
        self,
        table: TableName,
        key_condition: KeyCondition,
        *,
        start_key: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Condition] = None,
        scan_forward: bool = True,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        projection: Optional[ProjectionExpression] = None,
        select: Select = Select.all_attributes,
    ) -> Page:
        """
        Query a single DynamoDB page.
        To automatically handle pagination, uses query(...) instead.
        """
        payload = _query_payload(
            table=table,
            key_condition=key_condition,
            start_key=start_key,
            filter_expression=filter_expression,
            scan_forward=scan_forward,
            index=index,
            projection=projection,
            select=select,
        )
        if limit is not None:
            payload["Limit"] = limit

        response = await self.send_request(action="Query", payload=payload)

        last_evaluated_key: Optional[Dict[str, Any]]
        try:
            last_evaluated_key = dy2py(response["LastEvaluatedKey"], self.numeric_type)
        except KeyError:
            last_evaluated_key = None

        return Page(
            items=[dy2py(item, self.numeric_type) for item in response["Items"]],
            last_evaluated_key=last_evaluated_key,
        )

    async def scan(
        self,
        table: TableName,
        *,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        start_key: Optional[Dict[str, Any]] = None,
        projection: Optional[ProjectionExpression] = None,
        filter_expression: Optional[Condition] = None,
    ) -> AsyncIterator[Item]:
        """
        Scan the table.
        Unlike the DynamoDB API, the results are automatically de-paginated
        and a single stream of items is returned. For manual pagination, use
        scan_single_page(...) instead.
        """

        payload = _scan_payload(
            table=table,
            index=index,
            start_key=start_key,
            projection=projection,
            filter_expression=filter_expression,
        )

        async for result in self._depaginate("Scan", payload, limit):
            for item in result["Items"]:
                yield dy2py(item, self.numeric_type)

    async def scan_single_page(
        self,
        table: TableName,
        *,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        start_key: Optional[Dict[str, Any]] = None,
        projection: Optional[ProjectionExpression] = None,
        filter_expression: Optional[Condition] = None,
    ) -> Page:
        """
        Scan a single DynamoDB page.
        To automatically handle pagination, uses scan(...) instead.
        """

        payload = _scan_payload(
            table=table,
            index=index,
            start_key=start_key,
            projection=projection,
            filter_expression=filter_expression,
        )

        if limit is not None:
            payload["Limit"] = limit

        response = await self.send_request(action="Scan", payload=payload)

        last_evaluated_key: Optional[Dict[str, Any]]
        try:
            last_evaluated_key = dy2py(response["LastEvaluatedKey"], self.numeric_type)
        except KeyError:
            last_evaluated_key = None

        return Page(
            items=[dy2py(item, self.numeric_type) for item in response["Items"]],
            last_evaluated_key=last_evaluated_key,
        )

    async def count(
        self,
        table: TableName,
        key_condition: KeyCondition,
        *,
        start_key: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Condition] = None,
        index: Optional[str] = None,
    ) -> int:
        params = Parameters()

        payload: Dict[str, Any] = {
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

        payload.update(params.to_request_payload())

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
        condition: Optional[Condition] = None,
    ) -> Union[Item, None]:
        params = Parameters()

        expression = update_expression.encode(params)
        if not expression:
            raise EmptyItem()

        payload: Dict[str, Any] = {
            "TableName": table,
            "Key": py2dy(key),
            "UpdateExpression": expression,
            "ReturnValues": return_values.value,
        }
        if condition:
            payload["ConditionExpression"] = condition.encode(params)

        payload.update(params.to_request_payload())

        resp = await self.send_request(action="UpdateItem", payload=payload)

        if "Attributes" in resp:
            return dy2py(resp["Attributes"], self.numeric_type)
        else:
            return None

    async def batch_get(
        self, request: Dict[TableName, BatchGetRequest]
    ) -> BatchGetResponse:
        payload = {
            "RequestItems": {
                table: get_request.to_request_payload()
                for table, get_request in request.items()
            }
        }
        response = await self.send_request(action="BatchGetItem", payload=payload)
        return BatchGetResponse(
            items={
                table: [dy2py(item, self.numeric_type) for item in items]
                for table, items in response["Responses"].items()
            },
            unprocessed_keys={
                table: [dy2py(key, self.numeric_type) for key in unprocessed["Keys"]]
                for table, unprocessed in response["UnprocessedKeys"].items()
            },
        )

    async def batch_write(
        self, request: Dict[TableName, BatchWriteRequest]
    ) -> Dict[TableName, BatchWriteResult]:
        payload = {
            "RequestItems": {
                table_name: write_request.to_request_payload()
                for table_name, write_request in request.items()
            }
        }
        response = await self.send_request(action="BatchWriteItem", payload=payload)
        result = {}
        for table, items in response["UnprocessedItems"].items():
            undeleted_keys = []
            unput_items = []
            for item in items:
                try:
                    undeleted_keys.append(
                        dy2py(item["DeleteRequest"]["Key"], self.numeric_type)
                    )
                except KeyError:
                    unput_items.append(
                        dy2py(item["PutRequest"]["Item"], self.numeric_type)
                    )
            result[table] = BatchWriteResult(undeleted_keys, unput_items)
        return result

    async def send_request(
        self,
        *,
        action: str,
        payload: Mapping[str, Any],
    ) -> Dict[str, Any]:
        failed: Optional[Exception] = None
        try:
            async for _ in self.throttle_config.attempts():
                key = await self.credentials.get_key(self.http)
                if key is None:
                    logger.debug("no credentials found")
                    failed = NoCredentialsFound()
                    continue
                request = signed_dynamo_request(
                    key=key,
                    payload=payload,
                    action=action,
                    region=self.region,
                    endpoint=self.endpoint,
                )
                try:
                    logger.debug("sending request %r", request)
                    return await self.http.post(
                        url=request.url, headers=request.headers, body=request.body
                    )
                except Throttled:
                    logger.debug("request throttled")
                except ProvisionedThroughputExceeded:
                    logger.debug("provisioned throughput exceeded")
                except ExpiredToken:
                    logger.debug("token expired")
                    if not self.credentials.invalidate():
                        raise
                except RequestFailed as exc:
                    logger.debug("request failed")
                    failed = exc
                except ServiceUnavailable:
                    logger.debug("service unavailable")
                except InternalDynamoError:
                    logger.debug("internal dynamo error")
        except Throttled:
            if failed is not None:
                raise failed
            raise
        raise BrokenThrottleConfig()

    async def _depaginate(
        self, action: str, payload: Dict[str, Any], limit: Optional[int] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Internal API to depaginate the results from query/scan/count.
        Don't call this directly, use .query, .scan or .count instead.
        """
        if limit is not None:
            payload = {**payload, "Limit": limit}
        task: Optional[asyncio.Task[Dict[str, Any]]] = asyncio.create_task(
            self.send_request(action=action, payload=payload)
        )
        try:
            while task:
                result = await task
                try:
                    payload = {
                        **payload,
                        "ExclusiveStartKey": result["LastEvaluatedKey"],
                    }
                except KeyError:
                    task = None
                else:
                    if limit is not None:
                        limit -= len(result["Items"])
                        if limit > 0:
                            payload["Limit"] = limit
                        else:
                            task = None
                if task:
                    task = asyncio.create_task(
                        self.send_request(action=action, payload=payload)
                    )
                yield result
        except asyncio.CancelledError:
            if task:
                task.cancel()
            raise


def _query_payload(
    *,
    table: TableName,
    key_condition: KeyCondition,
    start_key: Optional[Dict[str, Any]],
    filter_expression: Optional[Condition],
    scan_forward: bool,
    index: Optional[str],
    projection: Optional[ProjectionExpression],
    select: Select = Select.all_attributes,
) -> Dict[str, Any]:
    if projection:
        select = Select.specific_attributes
    if select is Select.count:
        raise TypeError("Cannot use Select.count with query, use count instead")

    params = Parameters()

    payload: Dict[str, Any] = {
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

    payload.update(params.to_request_payload())
    return payload


def _scan_payload(
    *,
    table: TableName,
    index: Optional[str],
    start_key: Optional[Dict[str, Any]],
    projection: Optional[ProjectionExpression],
    filter_expression: Optional[Condition],
) -> Dict[str, Any]:
    params = Parameters()

    payload: Dict[str, Any] = {
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

    payload.update(params.to_request_payload())
    return payload
