"""Wrapper around boto3 to make it simpler to use."""

import boto3
import botocore
import io
import logging


from botocore.exceptions import ClientError
from dataclasses import dataclass, field
from typing import Generator, Optional


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BucketLocation:
    """Definition of a reference to an S3 bucket."""

    name: str = "s3os"
    region: Optional[str] = None


@dataclass(frozen=True)
class ObjectLocation:
    """Definition of a reference to a single S3 object."""

    key: str
    bucket: BucketLocation = field(default_factory=BucketLocation)


def create_bucket(bucket: BucketLocation) -> None:
    """
    Create an S3 bucket in a specified region.

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket: BucketLocation to create
    """
    try:
        if bucket.region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket.name)
        else:
            s3_client = boto3.client("s3", region_name=bucket.region)
            location = {"LocationConstraint": bucket.region}
            s3_client.create_bucket(
                Bucket=bucket.name, CreateBucketConfiguration=location
            )
    except ClientError as e:
        log.error(f"Failed to create bucket {bucket!r}. {e}")
        raise


def bucket_exists(bucket: BucketLocation) -> bool:
    """Return True if the given bucket exists. Otherwise False."""
    s3_client = boto3.client("s3")
    response = s3_client.list_buckets()

    return bucket.name in response["Buckets"]


def ensure_bucket(bucket: BucketLocation) -> None:
    """
    Ensure the given bucket exists.

    If the bucket already exists, then no action is taken.
    Otherwise, the bucket is created.
    """
    if not bucket_exists(bucket):
        create_bucket(bucket)


def upload_object(object_location: ObjectLocation, stream: io.BytesIO) -> None:
    """
    Upload the given data stream as an object to s3.

    :param object_location: Location of the object to create/update.
    :param stream: Byte steam of the object data.
    """
    s3 = boto3.client("s3")
    result = s3.upload_fileobj(stream, object_location.bucket.name, object_location.key)
    log.debug(f"Result of upload to {object_location}: {result}")


def download_object(object_location: ObjectLocation) -> io.BytesIO:
    """
    Download the given object from s3.

    Raises KeyError if the object could not be found.

    :param object_location: Location of the object to download.
    :return: Byte stream of the object data.
    """
    s3 = boto3.client("s3")
    stream = io.BytesIO()
    try:
        result = s3.download_fileobj(
            object_location.bucket.name, object_location.key, stream
        )
    except botocore.exceptions.ClientError as err:
        if err.response.get("Error", {}).get("Message", "Unknown") == "Not Found":
            log.debug(f"S3 object {object_location} does not exist.")
            raise KeyError(f"S3 object {object_location} does not exist.") from err
        raise

    stream.seek(0)
    log.debug(f"Result of download from {object_location}: {result}")
    return stream


def delete_object(object_location: ObjectLocation) -> None:
    """
    Download the given object from s3.

    :param object_location: Location of the object to delete.
    """
    s3 = boto3.client("s3")
    result = s3.delete_object(
        Bucket=object_location.bucket.name, Key=object_location.key
    )
    log.debug(f"Result of delete of {object_location}: {result}")


def generate_items_in_bucket(
    bucket: BucketLocation, prefix: Optional[str] = None
) -> Generator[ObjectLocation, None, None]:
    """
    Generate all object locations in a bucket.

    :param bucket: BucketLocation to inspect.
    :param prefix: Optional string prefix to filter the objects in the bucket by.
    :return: Generator of ObjectLocation for each object in the bucket.
    """
    s3 = boto3.client("s3")

    kwargs = {"Bucket": bucket.name}

    if prefix is not None:
        kwargs["Prefix"] = prefix

    while True:
        response = s3.list_objects(**kwargs)

        for obj in response["Contents"]:
            yield ObjectLocation(key=obj["Key"], bucket=bucket)

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break
