import json

# TODO: move
from aiodynamo.errors import TableNotFound


class AIODynamoError(Exception):
    pass


class UnknownError(AIODynamoError):
    def __init__(self, status: int, body: bytes):
        self.status = status
        self.body = body
        super().__init__(body)


class UnknownOperation(AIODynamoError):
    pass


ERRORS = {
    "ResourceNotFoundException": TableNotFound,
    "UnknownOperationException": UnknownOperation,
}


def exception_from_response(status: int, body: bytes) -> Exception:
    try:
        return ERRORS[json.loads(body)["__type"].split("#", 1)[1]]()
    except:
        raise UnknownError(status, body)
