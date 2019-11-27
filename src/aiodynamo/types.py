from typing import Any, Callable, Dict, List, TypeVar, Union

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

Item = TypeVar("Item", bound=Dict[str, Any])
DynamoItem = TypeVar("DynamoItem", bound=Dict[str, Dict[str, Any]])
TableName = TypeVar("TableName", bound=str)
Path = List[Union[str, int]]
PathEncoder = Callable[[Path], str]
EncoderFunc = Callable[[Any], str]
NOTHING = object()
EMPTY = object()


class BinaryTypeDeserializer(TypeDeserializer):
    def _deserialize_b(self, value):
        return value


Serializer = TypeSerializer()
Deserializer = BinaryTypeDeserializer()
