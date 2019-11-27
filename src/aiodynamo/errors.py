class AioDynamoException(Exception):
    pass


class ItemNotFound(AioDynamoException):
    pass


class TableNotFound(AioDynamoException):
    pass


class EmptyItem(AioDynamoException):
    pass
