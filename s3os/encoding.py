"""Definition of dump and loading operations for storing objects in s3."""

import io

from ruamel import yaml

from typing import Any


def object_to_yaml_stream(obj: Any) -> io.BytesIO:
    """Convert the given object into a YAML byte stream."""
    stream = io.BytesIO()
    yaml.safe_dump(obj, stream=stream, encoding="utf-8")
    stream.seek(0)
    return stream


def object_from_yaml_stream(stream: io.BytesIO) -> Any:
    """Create an object from a stream of YAML bytes."""
    return yaml.safe_load(stream)
