"""Tests for the S3Dict object."""

import pytest

from mock import MagicMock, call

from s3os.s3_dict import S3Dict, S3DictConfig
from s3os.s3_wrapper import ObjectLocation


@pytest.fixture
def mock_s3_api(mocker):
    """Create mocked versions of the s3 API."""
    mocked_store = mocker.patch("s3os.s3_dict.store")
    mocked_retrieve = mocker.patch("s3os.s3_dict.retrieve")
    mocked_delete = mocker.patch("s3os.s3_dict.delete")
    return mocked_store, mocked_retrieve, mocked_delete


def assert_no_calls(*mocks: MagicMock) -> None:
    """Assert that none of the given mocks were called."""
    for mock in mocks:
        mock.assert_not_called()


def reset_all_mocks(*mocks: MagicMock) -> None:
    """Reset all the given mocks."""
    for mock in mocks:
        mock.reset_mock()


def test_s3_dict_config(subtests):
    """Test the S3ConfigDict."""
    with subtests.test("ID is defaulted to unique id."):
        c1 = S3DictConfig()
        c2 = S3DictConfig()
        assert c1.id != c2.id

    with subtests.test("ID not defaulted when given."):
        c = S3DictConfig(id="test")
        assert c.id == "test"

    with subtests.test("`s3_prefix` can be generated correctly."):
        c = S3DictConfig(id="test")
        assert c.s3_prefix == "test/"


def test_s3_dict_init_no_items(subtests, mock_s3_api):
    """Tests for creating an S3Dict without initial items."""
    m_store, m_retrieve, m_delete = mock_s3_api

    with subtests.test("Can be created with no arguments."):
        S3Dict()
        assert_no_calls(*mock_s3_api)

    with subtests.test("Can be created with a config object."):
        dic = S3Dict(_config=S3DictConfig(id="s3os_test"))
        assert dic._config.id == "s3os_test"
        assert_no_calls(*mock_s3_api)


@pytest.mark.parametrize("use_cache", [True, False])
@pytest.mark.parametrize(
    "init_items", [{"a": 2, "b": [1, 2]}, (("a", 2), ("b", [1, 2]))],
)
def test_s3_dict_init_with_items(subtests, mock_s3_api, init_items, use_cache):
    """
    Tests for creating an S3Dict with initial items.

    This implicitly tests the `update` method.
    """
    m_store, m_retrieve, m_delete = mock_s3_api
    config = S3DictConfig(id="s3os_test", use_cache=use_cache)

    if isinstance(init_items, dict):
        # https://github.com/python/mypy/issues/2582
        dic = S3Dict(**init_items, _config=config)  # type: ignore
    else:
        dic = S3Dict(init_items, _config=config)

    with subtests.test("Items are cached locally."):
        # Check against the inner data dict so we definitely don't
        # re-discover the keys from s3.
        if use_cache:
            assert "a" in dic.data and dic.data["a"] == 2
            assert "b" in dic.data and dic.data["b"] == [1, 2]
        else:
            assert "a" not in dic.data
            assert "b" not in dic.data

    with subtests.test("Items are uploaded to s3."):
        m_store.assert_has_calls(
            [
                call(ObjectLocation("s3os_test/a"), 2),
                call(ObjectLocation("s3os_test/b"), [1, 2]),
            ],
            any_order=True,
        )
        assert_no_calls(m_retrieve, m_delete)


def test_convert_key(subtests):
    """Test that key conversions are symmetric."""
    dic = S3Dict(_config=S3DictConfig(id="s3os_test"))

    with subtests.test("Test convert_to_s3_key."):
        assert dic.convert_to_s3_key("mykey") == "s3os_test/mykey"

    with subtests.test("Test convert_from_s3_key."):
        assert dic.convert_from_s3_key("s3os_test/mykey") == "mykey"

    with subtests.test(
        "Test that convert_from_s3_key only replaces at start of string."
    ):
        assert dic.convert_from_s3_key("asdf/mykey") == "asdf/mykey"
        assert dic.convert_from_s3_key("s3os_test/s3os_test/mykey") == "s3os_test/mykey"


@pytest.mark.parametrize("use_cache", [True, False])
def test_setitem(subtests, mock_s3_api, use_cache):
    """Test the __setitem__ method of S3Dict."""
    m_store, m_retrieve, m_delete = mock_s3_api
    dic = S3Dict(_config=S3DictConfig(id="s3os_test", use_cache=use_cache))

    dic["set"] = 5
    m_store.assert_has_calls([call(ObjectLocation("s3os_test/set"), 5)])

    # Check against the inner "data" dict
    if use_cache:
        assert dic.data["set"] == 5
    else:
        assert "set" not in dic.data

    assert_no_calls(m_retrieve, m_delete)


@pytest.mark.parametrize("use_cache", [True, False])
def test_getitem(subtests, mock_s3_api, use_cache):
    """Test the __getitem__ method of S3Dict."""
    m_store, m_retrieve, m_delete = mock_s3_api
    dic = S3Dict(_config=S3DictConfig(id="s3os_test", use_cache=use_cache))

    # Initialise some data in the dict, and then reset the store mock so we can
    # check it more easily later.
    dic["get"] = 12
    m_store.reset_mock()
    # Pretend that we actually did upload the object to s3.
    m_retrieve.return_value = 12

    # Actually perform the tests.
    value = dic["get"]
    assert value == 12
    if use_cache:
        assert_no_calls(*mock_s3_api)
    else:
        m_retrieve.assert_has_calls([call(ObjectLocation("s3os_test/get"))])
        assert_no_calls(m_store, m_delete)


@pytest.mark.parametrize("use_cache", [True, False])
def test_delitem(subtests, mock_s3_api, use_cache):
    """Test the __delitem__ method of S3Dict."""
    m_store, m_retrieve, m_delete = mock_s3_api
    dic = S3Dict(_config=S3DictConfig(id="s3os_test", use_cache=use_cache))

    dic["del"] = 7

    # Reset all the mocks after initialisation
    reset_all_mocks(*mock_s3_api)

    with subtests.test("Key is deleted from both s3 and cache."):
        del dic["del"]
        m_delete.assert_has_calls([call(ObjectLocation("s3os_test/del"))])
        assert_no_calls(m_retrieve, m_store)
        assert "del" not in dic.data

    with subtests.test("No error when key does not exist."):
        del dic["del2"]


@pytest.mark.parametrize("use_cache", [True, False])
def test_get_all_from_s3(subtests, mock_s3_api, mocker, use_cache):
    """Test the `get_all_from_s3` method."""
    m_store, m_retrieve, m_delete = mock_s3_api
    location_gen = (ObjectLocation(str(i)) for i in range(3))
    mock_generate_items_in_bucket = mocker.patch(
        "s3os.s3_dict.generate_items_in_bucket", return_value=location_gen,
    )
    m_retrieve.side_effect = [str(i * i) for i in range(3)]

    s3dict = S3Dict(_config=S3DictConfig(id="s3os_test", use_cache=use_cache))

    normal_dict = s3dict.get_all_from_s3()

    assert normal_dict == {"0": "0", "1": "1", "2": "4"}

    mock_generate_items_in_bucket.assert_called_once()
    assert_no_calls(m_store, m_delete)


def test_del(mock_s3_api):
    """Test that __del__ does not delete items from s3."""
    # Initialise with some data so that it could be deleted.
    s3dict = S3Dict({"a": 1, "b": 2}, _config=S3DictConfig(id="s3os_test"))

    # Reset all the mocks after initialisation
    reset_all_mocks(*mock_s3_api)
    del s3dict
    # No calls should have been made.
    assert_no_calls(*mock_s3_api)


def test_clear(mocker, mock_s3_api):
    """Test the `clear` method."""
    m_store, m_retrieve, m_delete = mock_s3_api
    mock_generate_items_in_bucket = mocker.patch(
        "s3os.s3_dict.generate_items_in_bucket",
        return_value=(ObjectLocation(f"s3os_test/{str(i)}") for i in range(3)),
    )
    s3dict = S3Dict(_config=S3DictConfig(id="s3os_test"))

    s3dict.clear()

    m_delete.assert_has_calls(
        [
            call(ObjectLocation("s3os_test/0")),
            call(ObjectLocation("s3os_test/1")),
            call(ObjectLocation("s3os_test/2")),
        ]
    )
    mock_generate_items_in_bucket.assert_called_once()
    assert_no_calls(m_store, m_retrieve)
