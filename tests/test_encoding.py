"""Test encoding methods used to store/retrieve objects."""

import pytest

from s3os.encoding import object_from_yaml_stream, object_to_yaml_stream


@pytest.mark.parametrize(
    "obj", ["asdf", [1, 2, 3], {1: 2, 3: 4}, 5],
)
def test_yaml_to_stream_conversion(obj):
    """Test that a variety of objects can be round-trip translated to/from YAML."""
    assert object_from_yaml_stream(object_to_yaml_stream(obj)) == obj
