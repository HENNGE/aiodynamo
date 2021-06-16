from __future__ import annotations

import abc
import asyncio
import datetime
import random
import time
from dataclasses import dataclass
from enum import Enum, unique
from itertools import count
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    cast,
)

from .errors import Throttled
from .expressions import Parameters, ProjectionExpression
from .types import (
    EncodedGlobalSecondaryIndex,
    EncodedKeySchema,
    EncodedLocalSecondaryIndex,
    EncodedProjection,
    EncodedStreamSpecification,
    EncodedThroughput,
    Item,
    TableName,
    Timespan,
)
from .utils import py2dy


@unique
class TimeToLiveStatus(Enum):
    enabling = "ENABLING"
    disabling = "DISABLING"
    enabled = "ENABLED"
    disabled = "DISABLED"


@dataclass(frozen=True)
class TimeToLiveDescription:
    table: str
    attribute: str
    status: TimeToLiveStatus


@dataclass(frozen=True)
class Throughput:
    read: int
    write: int

    def encode(self) -> EncodedThroughput:
        return {"ReadCapacityUnits": self.read, "WriteCapacityUnits": self.write}


class KeyType(Enum):
    string = "S"
    number = "N"
    binary = "B"


@dataclass(frozen=True)
class KeySpec:
    name: str
    type: KeyType


@dataclass(frozen=True)
class KeySchema:
    hash_key: KeySpec
    range_key: Optional[KeySpec] = None

    def __iter__(self) -> Iterator[KeySpec]:
        yield self.hash_key

        if self.range_key:
            yield self.range_key

    def to_attributes(self) -> Dict[str, str]:
        return {key.name: key.type.value for key in self}

    def encode(self) -> List[EncodedKeySchema]:
        return [
            {"AttributeName": key.name, "KeyType": key_type}
            for key, key_type in zip(self, ["HASH", "RANGE"])
        ]


class ProjectionType(Enum):
    all = "ALL"
    keys_only = "KEYS_ONLY"
    include = "INCLUDE"


@dataclass(frozen=True)
class Projection:
    type: ProjectionType
    attrs: Optional[List[str]] = None

    def encode(self) -> EncodedProjection:
        encoded: EncodedProjection = {"ProjectionType": self.type.value}
        if self.attrs:
            encoded["NonKeyAttributes"] = self.attrs
        return encoded


@dataclass(frozen=True)
class LocalSecondaryIndex:
    name: str
    schema: KeySchema
    projection: Projection

    def encode(self) -> EncodedLocalSecondaryIndex:
        return {
            "IndexName": self.name,
            "KeySchema": self.schema.encode(),
            "Projection": self.projection.encode(),
        }


@dataclass(frozen=True)
class GlobalSecondaryIndex(LocalSecondaryIndex):
    throughput: Throughput

    def encode(self) -> EncodedGlobalSecondaryIndex:
        # Cast due to https://github.com/python/mypy/issues/4122
        return cast(
            EncodedGlobalSecondaryIndex,
            {**super().encode(), "ProvisionedThroughput": self.throughput.encode()},
        )


class StreamViewType(Enum):
    keys_only = "KEYS_ONLY"
    new_image = "NEW_IMAGE"
    old_image = "OLD_IMAGE"
    new_and_old_images = "NEW_AND_OLD_IMAGES"


@dataclass(frozen=True)
class StreamSpecification:
    enabled: bool = False
    view_type: StreamViewType = StreamViewType.new_and_old_images

    def encode(self) -> EncodedStreamSpecification:
        spec: EncodedStreamSpecification = {"StreamEnabled": self.enabled}
        if self.enabled:
            spec["StreamViewType"] = self.view_type.value
        return spec


class ReturnValues(Enum):
    none = "NONE"
    all_old = "ALL_OLD"
    updated_old = "UPDATED_OLD"
    all_new = "ALL_NEW"
    updated_new = "UPDATED_NEW"


class TableStatus(Enum):
    creating = "CREATING"
    updating = "UPDATING"
    deleting = "DELETING"
    active = "ACTIVE"


@dataclass(frozen=True)
class TableDescription:
    attributes: Optional[Dict[str, KeyType]]
    created: Optional[datetime.datetime]
    item_count: Optional[int]
    key_schema: Optional[KeySchema]
    throughput: Optional[Throughput]
    status: TableStatus


class Select(Enum):
    all_attributes = "ALL_ATTRIBUTES"
    all_projected_attributes = "ALL_PROJECTED_ATTRIBUTES"
    count = "COUNT"
    specific_attributes = "SPECIFIC_ATTRIBUTES"


@dataclass(frozen=True)
class WaitConfig:
    max_attempts: int
    retry_delay: Timespan

    @classmethod
    def default(cls) -> WaitConfig:
        return WaitConfig(25, 20)

    async def attempts(self) -> AsyncIterator[None]:
        for _ in range(self.max_attempts):
            yield
            await asyncio.sleep(self.retry_delay)


@dataclass(frozen=True)
class MypyWorkaroundThrottleConfigBase:
    time_limit_secs: Timespan = 60


class ThrottleConfig(MypyWorkaroundThrottleConfigBase, metaclass=abc.ABCMeta):
    @classmethod
    def default(cls) -> ThrottleConfig:
        return ExponentialBackoffThrottling()

    @abc.abstractmethod
    def delays(self) -> Iterable[float]:
        raise NotImplementedError()

    async def attempts(self) -> AsyncIterable[None]:
        deadline = time.monotonic() + self.time_limit_secs
        for delay in self.delays():
            yield
            if time.monotonic() > deadline:
                raise Throttled()
            await asyncio.sleep(delay)


@dataclass(frozen=True)
class DecorelatedJitterThrottling(ThrottleConfig):
    max_time_secs: Timespan = 60
    base_delay_secs: Timespan = 0.05
    max_delay_secs: Timespan = 1

    def delays(self) -> Iterator[Timespan]:
        current_delay_secs = self.base_delay_secs
        while True:
            current_delay_secs = min(
                self.max_delay_secs,
                random.uniform(self.base_delay_secs, current_delay_secs * 3),
            )
            yield current_delay_secs


@dataclass(frozen=True)
class ExponentialBackoffThrottling(ThrottleConfig):
    base_delay_secs: Timespan = 2
    max_delay_secs: Timespan = 20

    def delays(self) -> Iterable[float]:
        for attempt in count():
            yield min(
                random.random() * (self.base_delay_secs ** attempt), self.max_delay_secs
            )


@dataclass(frozen=True)
class Page:
    items: List[Item]
    last_evaluated_key: Optional[Dict[str, Any]]

    @property
    def is_last_page(self) -> bool:
        return self.last_evaluated_key is None


@dataclass(frozen=True)
class BatchGetRequest:
    keys: List[Item]
    projection: Optional[ProjectionExpression] = None

    def to_request_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "Keys": [py2dy(key) for key in self.keys],
        }
        if self.projection:
            params = Parameters()
            payload["ProjectionExpression"] = self.projection.encode(params)
            payload.update(params.to_request_payload())
        return payload


@dataclass(frozen=True)
class BatchGetResponse:
    items: Dict[TableName, List[Item]]
    unprocessed_keys: Dict[TableName, List[Item]]


@dataclass(frozen=True)
class BatchWriteRequest:
    keys_to_delete: Optional[List[Item]] = None
    items_to_put: Optional[List[Item]] = None

    def to_request_payload(self) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        if self.keys_to_delete:
            payload.extend(
                {"DeleteRequest": {"Key": py2dy(key)}} for key in self.keys_to_delete
            )
        if self.items_to_put:
            payload.extend(
                {"PutRequest": {"Item": py2dy(item)}} for item in self.items_to_put
            )
        return payload


@dataclass(frozen=True)
class BatchWriteResult:
    undeleted_keys: List[Item]
    unput_items: List[Item]
