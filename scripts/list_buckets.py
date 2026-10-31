"""List every object in the pipeline buckets (evidence for the object store test).

Runs inside any service container, which has boto3 and the S3 environment:

    docker compose exec -T api-gateway python - < scripts/list_buckets.py
    kubectl exec -i -n aic deploy/api-gateway -- python - < scripts/list_buckets.py
"""

import os

import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    region_name=os.environ.get("S3_REGION", "garage"),
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
)

for bucket in ("measurements", "reports"):
    objects = [o for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket)
               for o in page.get("Contents", [])]
    print(f"{bucket}/ ({len(objects)} objects)")
    for o in objects:
        print(f"  {o['Key']:<45} {o['Size']:>7} B  {o['LastModified']:%Y-%m-%d %H:%M:%S}")
