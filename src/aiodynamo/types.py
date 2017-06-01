from decimal import Decimal
from typing import Union, Set, List, Dict, TypeVar, Type

DynamoValue = Union[
    None,
    bool,
    int,
    Decimal,
    str,
    bytes,
    Set[int],
    Set[str],
    Set[bytes],
    List['DynamoValue'],
    Dict[str, 'DynamoValue']
]

DynamoKeyType = Union[Type[str], Type[bytes], Type[int]]

DynamoObject = Dict[str, DynamoValue]

EncodedValue = Union[
    bool,
    str,
    bytes,
    List[str],
    List[bytes],
    Dict[str, 'EncodedObject']
]
EncodedObject = Dict[str, Dict[str, EncodedValue]]

TModel = TypeVar('TModel')
