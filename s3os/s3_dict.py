"""Definition of a dict-like interface to s3."""

from collections import UserDict
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from uuid import uuid4

from s3os.api import store, retrieve, delete
from s3os.s3_wrapper import BucketLocation, ObjectLocation, generate_items_in_bucket


@dataclass
class S3DictConfig:
    """
    Configuration options for S3Dict.

    :param id: ID of the dict. Used to distinguish dicts within the same bucket.
        If not given, a universally unique identifier will be generated using UUID4.
        Objects in s3 can be retrieved by multiple S3Dict instances by using the same `id`.
    :param use_cache:
        Optionally data can be cached locally for improved speed. If `use_cache` is True then:

            - Set operations immediately upload to s3.
            - Get operations only download from s3 if they haven't been previously downloaded.
            - Delete operations immediately delete objects in s3.

        If `use_cache` is False then:

            - Set operations immediately upload to s3.
            - Get operations synchronously download objects from s3.
            - Delete operations immediately delete objects in s3.
    :param bucket: Optional. The s3 bucket to use.
    """

    id: str = field(default_factory=lambda: str(uuid4()))
    use_cache: bool = True
    bucket: BucketLocation = field(default_factory=BucketLocation)

    @property
    def s3_prefix(self):
        """The prefix to use for all items stored by this dict."""
        return f"{self.id}/"


class S3Dict(UserDict):
    """
    Provides a dict-like interface to objects stored in s3.

    Provides no guarantees or safety for concurrent access of objects.

    See S3DictConfig for configuration options.

    Deviations from the standard dict API:
      - del["item"] when "item" does not exist does not raise a KeyError.
      - __del__ doesn't delete all keys in s3 automatically. Use `clear()` to do this.
            This is different to normal behaviour where python garbage collection
            would delete all the items in a dictionary if they are no longer referenced.
    """

    def __init__(
        self, *args: Any, _config: Optional[S3DictConfig] = None, **kwargs: Any,
    ):
        """
        Create a new S3Dict.

        :param args: Standard `dict` arguments.
        :param _config: Optional S3DictConfig object. See S3DictConfig for default behaviour.
        :param kwargs: Standard `dict` keyword arguments.
        """
        if _config is None:
            _config = S3DictConfig()
        elif not isinstance(_config, S3DictConfig):
            raise ValueError(
                f"`_config` must be of type S3DictConfig or None. You passed: {_config=}."
            )
        self._config: S3DictConfig = _config

        # Call super after setting self._config so objects can be stored immediately.
        super(S3Dict, self).__init__(*args, **kwargs)

    def get_all_from_s3(self) -> Dict[str, Any]:
        """
        Discover all objects stored in s3 using this dict's ID.

        Caches the result on this object if configured to do so.

        Returns a dict of the discovered objects.
        """
        object_generator = generate_items_in_bucket(
            self._config.bucket, prefix=self._config.s3_prefix
        )

        all_items = dict()

        for object_location in object_generator:
            value = retrieve(object_location)
            key = self.convert_from_s3_key(object_location.key)
            all_items[key] = value

        if self._config.use_cache:
            # Update `data` directly rather than `self` so we don't just re-write
            # the keys back to s3 again.
            self.data.update(all_items)

        return all_items

    @property
    def as_dict(self) -> Dict[str, Any]:
        """
        Return a normal dict containing the information stored in s3 under this dict.

        Can be used on both cached and non-cached S3Dicts.

        NB: For cached dicts this will only return what is in the cache.
            Use `get_all_from_s3()` to force population of the entire dict.
        """
        if self._config.use_cache:
            return self.data
        else:
            return self.get_all_from_s3()

    def convert_from_s3_key(self, s3_key: str) -> str:
        """Strips the prepended dict ID from the s3 key."""
        if s3_key.startswith(self._config.s3_prefix):
            return s3_key.replace(self._config.s3_prefix, "", 1)
        return s3_key

    def convert_to_s3_key(self, key: str) -> str:
        """Prepend this dicts ID to the key for unique identification in s3."""
        return f"{self._config.s3_prefix}{key}"

    def __setitem__(self, key: str, value: Any) -> None:
        """Store the item in s3, as well as in the cache if configured to do so."""
        object_location = ObjectLocation(
            key=self.convert_to_s3_key(key), bucket=self._config.bucket
        )
        store(object_location, value)

        if self._config.use_cache:
            super(S3Dict, self).__setitem__(key, value)

    def __getitem__(self, item: str) -> Any:
        """Get the item from s3, using the cache if configured to do so."""
        object_location = ObjectLocation(
            key=self.convert_to_s3_key(item), bucket=self._config.bucket
        )
        if self._config.use_cache:
            try:
                # Try find it locally.
                value = super(S3Dict, self).__getitem__(item)
            except KeyError:
                # On failure, grab it from s3
                # If it doesn't exist in s3, then this will raise a KeyError itself
                # which is normal behaviour for a Dict.
                value = retrieve(object_location)
                # Store it locally to cache it for next time
                super(S3Dict, self).__setitem__(item, value)
        else:
            value = retrieve(object_location)

        return value

    def __delitem__(self, item: str) -> None:
        """Delete the item from s3, as well as from the cache if configured to do so."""
        object_location = ObjectLocation(
            key=self.convert_to_s3_key(item), bucket=self._config.bucket
        )
        # As s3 delete operations are a no-op if it doesn't exist, then we can't
        # tell if the item existed already or not.
        # Therefore this is a departure from the normal `dict` API because we can't
        # raise a KeyError on failure to delete.
        delete(object_location)
        if self._config.use_cache:
            try:
                super(S3Dict, self).__delitem__(item)
            except KeyError:
                # To keep things consistent, also don't raise a KeyError
                # when deleting from the cache.
                pass

    def clear(self) -> None:
        """Delete all the objects stored in s3 under this dict and clear the cache."""
        object_generator = generate_items_in_bucket(
            self._config.bucket, prefix=self._config.s3_prefix
        )

        for object_location in object_generator:
            delete(object_location)

        super(S3Dict, self).clear()
