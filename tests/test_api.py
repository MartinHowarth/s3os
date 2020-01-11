"""Tests for the simplified s3 api."""

import pytest

from s3os.api import store, retrieve, delete
from s3os.s3_wrapper import ObjectLocation


@pytest.mark.parametrize(
    "obj,key",
    [("asdf", "string"), ([1, 2, 3], "list"), ({1: 2, 3: 4}, "dict"), (5, "int")],
)
def test_store_and_retrieve(obj, key):
    """Test that various object types can be stored and retrieved from s3."""
    location = ObjectLocation(key=key)
    store(location, obj)

    retrieved = retrieve(location)

    assert retrieved == obj


def test_failure_to_retrieve_missing_object():
    """Test that retrieving non-existent objects fails correctly."""
    with pytest.raises(KeyError):
        retrieve(ObjectLocation("DOES_NOT_EXIST"))


def test_failure_to_delete_missing_object():
    """Test that deleting non-existent objects fails correctly."""
    # No error expected - it's just a no-op.
    delete(ObjectLocation("DOES_NOT_EXIST"))
