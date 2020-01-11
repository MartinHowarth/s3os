"""Definition of the simplest API to s3."""

from typing import Any

from .s3_wrapper import (
    ObjectLocation,
    ensure_bucket,
    upload_object,
    download_object,
    delete_object,
)
from .encoding import object_from_yaml_stream, object_to_yaml_stream


def store(object_location: ObjectLocation, obj: Any) -> None:
    """
    Store the given object in s3 at the given location.

    :param object_location: Definition of the bucket and key to store the object under.
    :param obj: The object to store. Must be able to be dumped/loaded to/from YAML.
    """
    ensure_bucket(object_location.bucket)
    obj_stream = object_to_yaml_stream(obj)
    upload_object(object_location, obj_stream)


def retrieve(object_location: ObjectLocation) -> Any:
    """
    Retrieve the object stored in s3 at the given location.

    :param object_location: Definition of the bucket and key to download.
    :return: The object retrieved, as a native python object.
    """
    obj_stream = download_object(object_location)
    obj = object_from_yaml_stream(obj_stream)
    return obj


def delete(object_location: ObjectLocation) -> None:
    """
    Delete the object stored in s3 at the given location.

    :param object_location: Definition of the bucket and key to delete.
    """
    delete_object(object_location)


def store_simple(key: str, value: Any) -> None:
    """
    Store the given object in s3 under the given key.

    Stores items at the root of the default bucket "s3os" with no unique identifier.

    :param key: Key to refer to the object.
    :param value: Object to store.
    """
    store(value, ObjectLocation(key=key))


def retrieve_simple(key: str) -> Any:
    """
    Store the given object in s3 under the given key.

    :param key: Key to of the object to retrieve
    """
    return retrieve(ObjectLocation(key=key))


def delete_simple(key: str) -> None:
    """
    Delete the object from s3 under the given key.

    :param key: Key to refer to the object.
    """
    delete(ObjectLocation(key=key))
