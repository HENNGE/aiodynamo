from __future__ import annotations

import datetime
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import *

from yarl import URL

from .credentials import Key

SERVICE = "dynamodb"
CONTENT_TYPE = "application/x-amz-json-1.0"
API_VERSION = "DynamoDB_20120810"
METHOD = "POST"
ALGORITHM = "AWS4-HMAC-SHA256"


def make_default_endpoint(region: str) -> URL:
    return URL.build(scheme="https", host=f"{SERVICE}.{region}.amazonaws.com", path="/")


@dataclass(frozen=True)
class Instant:
    _dt: datetime.datetime

    @classmethod
    def now(cls) -> Instant:
        return cls(datetime.datetime.now(datetime.timezone.utc))

    @property
    def timestamp(self) -> str:
        return self._dt.strftime("%Y%m%dT%H%M%SZ")

    @property
    def date(self) -> str:
        return self._dt.strftime("%Y%m%d")


@dataclass(frozen=True)
class Request:
    url: URL
    headers: Dict[str, str] = field(repr=False)
    body: bytes


def derive_signing_key(key: Key, instant: Instant, region: str) -> bytes:
    tmp_key1 = sha256_hmac(("AWS4" + key.secret).encode("utf-8"), instant.date)
    tmp_key2 = sha256_hmac(tmp_key1, region)
    tmp_key3 = sha256_hmac(tmp_key2, SERVICE)
    return sha256_hmac(tmp_key3, "aws4_request")


def signed_dynamo_request(
    *,
    key: Key,
    payload: Mapping[str, Any],
    action: str,
    region: str,
    endpoint: Optional[URL] = None,
) -> Request:
    instant = Instant.now()
    endpoint = endpoint or make_default_endpoint(region)
    host = endpoint.host
    amz_target = f"{API_VERSION}.{action}"

    canonical_uri = "/"
    canonical_querystring = ""
    canonical_headers = (
        f"content-type:{CONTENT_TYPE}\n"
        f"host:{host}\n"
        f"x-amz-date:{instant.timestamp}\n"
        f"x-amz-target:{amz_target}\n"
    )

    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    signed_headers = "content-type;host;x-amz-date;x-amz-target"
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()
    canonical_request = (
        f"{METHOD}\n"
        f"{canonical_uri}\n"
        f"{canonical_querystring}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    credential_scope = f"{instant.date}/{region}/{SERVICE}/aws4_request"
    request_digest = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    string_to_sign = (
        f"{ALGORITHM}\n"
        f"{instant.timestamp}\n"
        f"{credential_scope}\n"
        f"{request_digest}"
    )

    signing_key = derive_signing_key(key, instant, region)
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    authorization_header = f"{ALGORITHM} Credential={key.id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

    headers = {
        "Content-Type": CONTENT_TYPE,
        "X-Amz-Date": instant.timestamp,
        "X-Amz-Target": amz_target,
        "Authorization": authorization_header,
    }
    if key.token:
        headers["X-Amz-Security-Token"] = key.token
    return Request(url=endpoint, headers=headers, body=payload_bytes)


def sha256_hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
