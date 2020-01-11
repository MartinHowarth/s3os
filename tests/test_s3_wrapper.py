"""Tests for s3 API wrapper functions."""

from s3os.api import retrieve
from s3os.s3_wrapper import generate_items_in_bucket, BucketLocation


def test_generate_items_in_bucket():
    """
    Test `generate_items_in_bucket`.

    NB: Does not actually test paging as that costs more to maintain in s3.
    """
    bucket = BucketLocation()
    items = [item for item in generate_items_in_bucket(bucket)]
    assert len(items) != 0

    # Download an object to make sure generation gave us valid items
    obj = retrieve(items[0])
    assert obj is not None
