"""Python package to make storing and retrieving python objects in s3 trivial."""
# flake8: noqa

from .api import store, retrieve, delete, store_simple, retrieve_simple, delete_simple
from .s3_dict import S3Dict, S3DictConfig
from .s3_wrapper import BucketLocation, ObjectLocation
