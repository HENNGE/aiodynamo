import base64


def generate_item(nest):
    item = {
        "hash": {
            "S": "string",
        },
        "range": {
            "B": base64.b64encode(b"bytes"),
        },
        "null": {"NULL": True},
        "true": {"BOOL": True},
        "false": {"BOOL": False},
        "int": {"N": "42"},
        "float": {"N": "4.2"},
        "numeric_set": {"NS": ["42", "4.2"]},
        "string_set": {"SS": ["hello", "world"]},
        "binary_set": {
            "BS": [base64.b64encode(b"hello"), base64.b64encode(b"world")]
        },
    }
    if nest:
        item["list"] = {"L": [{"M": generate_item(False)}]}
    return item


def generate_data(num_items=30_000):
    return [generate_item(True) for _ in range(num_items)]
