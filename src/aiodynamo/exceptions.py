class AioDynamoError(Exception):
    pass


class NotModified(AioDynamoError):
    pass


class NotFound(AioDynamoError):
    pass


class InvalidModel(AioDynamoError):
    pass


class InvalidKey(AioDynamoError):
    pass


class TableAlreadyExists(AioDynamoError):
    pass
