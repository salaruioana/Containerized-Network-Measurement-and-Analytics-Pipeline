"""Provider-agnostic S3 client helper for Garage, MinIO, or AWS S3.

See Task group-05#7.
Ensures path-style addressing is used for S3-compatible object stores (like Garage)
and provides common operations (get_json, put_json, exists) and key layout helpers.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from aic_common.contract import (
    Buckets,
    MEASUREMENT_PREFIX,
    REPORT_PREFIX,
)


class S3Error(Exception):
    """Base exception for S3 helper operations."""
    pass


class S3ObjectNotFoundError(S3Error, KeyError):
    """Raised when an S3 object is not found in a bucket."""
    pass


def get_s3_client(
    endpoint_url: str | None = None,
    region_name: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> Any:
    """Create a boto3 S3 client configured with path-style addressing.

    Parameters default to their respective environment variables if not provided:
    - endpoint_url: S3_ENDPOINT (default: 'http://garage:3900')
    - region_name: S3_REGION (default: 'garage')
    - aws_access_key_id: S3_ACCESS_KEY
    - aws_secret_access_key: S3_SECRET_KEY
    """
    endpoint = endpoint_url or os.getenv("S3_ENDPOINT", "http://garage:3900")
    region = region_name or os.getenv("S3_REGION", "garage")
    access_key = aws_access_key_id or os.getenv("S3_ACCESS_KEY")
    secret_key = aws_secret_access_key or os.getenv("S3_SECRET_KEY")

    config = Config(s3={"addressing_style": "path"})

    kwargs: dict[str, Any] = {
        "endpoint_url": endpoint,
        "region_name": region,
        "config": config,
    }
    if access_key:
        kwargs["aws_access_key_id"] = access_key
    if secret_key:
        kwargs["aws_secret_access_key"] = secret_key

    return boto3.client("s3", **kwargs)


def put_json(
    bucket: str,
    key: str,
    data: Any,
    client: Any | None = None,
) -> None:
    """Serialize data to JSON and upload it to the specified S3 bucket and key.

    Supports dicts, lists, primitives, and Pydantic models (via model_dump or dict).
    """
    if client is None:
        client = get_s3_client()

    if hasattr(data, "model_dump"):
        payload = json.dumps(data.model_dump(exclude_none=True), ensure_ascii=False)
    elif hasattr(data, "dict"):
        payload = json.dumps(data.dict(exclude_none=True), ensure_ascii=False)
    else:
        payload = json.dumps(data, ensure_ascii=False)

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )


def get_json(
    bucket: str,
    key: str,
    client: Any | None = None,
) -> Any:
    """Retrieve and deserialize a JSON object from the specified S3 bucket and key.

    Raises S3ObjectNotFoundError if the key or bucket does not exist.
    """
    if client is None:
        client = get_s3_client()

    try:
        response = client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("NoSuchKey", "NotFound", "NoSuchBucket") or status == 404:
            raise S3ObjectNotFoundError(f"Object '{key}' not found in bucket '{bucket}'") from exc
        raise


def exists(
    bucket: str,
    key: str,
    client: Any | None = None,
) -> bool:
    """Check if an object exists in the specified S3 bucket."""
    if client is None:
        client = get_s3_client()

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("NoSuchKey", "NotFound", "404") or status == 404:
            return False
        raise


def report_id_for(measurement_id: str) -> str:
    """Derive report_id from measurement_id (e.g. 'm-a1b2' -> 'r-a1b2')."""
    if not isinstance(measurement_id, str):
        raise ValueError("Invalid measurement_id: must be a string")
    if not measurement_id.startswith(MEASUREMENT_PREFIX):
        raise ValueError(f"Invalid measurement_id: must start with {MEASUREMENT_PREFIX}")
    suffix = measurement_id[len(MEASUREMENT_PREFIX):]
    if not suffix:
        raise ValueError("Invalid measurement_id: empty suffix")
    return f"{REPORT_PREFIX}{suffix}"


def measurement_key(source_host: str, target_host: str, measurement_id: str) -> str:
    """Generate deterministic S3 key for raw measurement (e.g. 'h1-h2/m-001.json')."""
    return f"{source_host}-{target_host}/{measurement_id}.json"


def measurement_object_path(source_host: str, target_host: str, measurement_id: str) -> str:
    """Generate object_path for Redis metadata (e.g. 'measurements/h1-h2/m-001.json')."""
    return f"{Buckets.MEASUREMENTS}/{measurement_key(source_host, target_host, measurement_id)}"


def report_key(report_id: str) -> str:
    """Generate deterministic S3 key for a report document (e.g. 'r-001.json')."""
    if report_id.startswith(MEASUREMENT_PREFIX):
        report_id = report_id_for(report_id)
    if report_id.endswith(".json"):
        return report_id
    return f"{report_id}.json"


def report_object_path(report_id: str) -> str:
    """Generate object_path for report document (e.g. 'reports/r-001.json')."""
    return f"{Buckets.REPORTS}/{report_key(report_id)}"

