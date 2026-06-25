"""Tests for the persistent output cache (issue #369)."""

import os

import pytest

from stimela.kitchen.cache import _CACHE_MISS, CacheConfig, OutputCache, _hash_inputs


@pytest.fixture
def cache_dir(tmp_path):
    """Provide a temporary directory for cache tests."""
    return str(tmp_path)


@pytest.fixture
def cache_config(cache_dir):
    """Provide a CacheConfig pointing at a temp directory."""
    return CacheConfig(enabled=True, dir=cache_dir, name=".test-cache")


class TestOutputCache:
    """Tests for OutputCache basic operations."""

    def test_store_and_lookup(self, cache_config):
        """Store a value and look it up; should match."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash123")
            value = cache.lookup("recipe1", "step1", "result", "hash123")
        assert value == 42

    def test_lookup_miss_empty_cache(self, cache_config):
        """Lookup on an empty cache returns _CACHE_MISS."""
        with OutputCache(cache_config) as cache:
            value = cache.lookup("recipe1", "step1", "result", "hash123")
        assert value is _CACHE_MISS

    def test_lookup_miss_wrong_hash(self, cache_config):
        """Lookup with a different input hash returns _CACHE_MISS."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash123")
            value = cache.lookup("recipe1", "step1", "result", "hash_different")
        assert value is _CACHE_MISS

    def test_lookup_miss_wrong_key(self, cache_config):
        """Lookup with a different key returns _CACHE_MISS."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash123")
            value = cache.lookup("recipe1", "step2", "result", "hash123")
        assert value is _CACHE_MISS

    def test_store_overwrite(self, cache_config):
        """Storing a new value for the same key overwrites the old one."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash1")
            cache.store("recipe1", "step1", "result", 99, "hash2")
            # old hash no longer matches
            assert cache.lookup("recipe1", "step1", "result", "hash1") is _CACHE_MISS
            # new hash returns new value
            assert cache.lookup("recipe1", "step1", "result", "hash2") == 99

    def test_store_complex_types(self, cache_config):
        """Cache can store and retrieve complex Python objects."""
        data = {"key": [1, 2, 3], "nested": {"a": True, "b": 3.14}}
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "data", data, "hash1")
            value = cache.lookup("recipe1", "step1", "data", "hash1")
        assert value == data

    def test_persistence_across_sessions(self, cache_config):
        """Cache persists across separate open/close cycles."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash1")

        # open again and verify
        with OutputCache(cache_config) as cache:
            value = cache.lookup("recipe1", "step1", "result", "hash1")
        assert value == 42

    def test_invalidate_specific_output(self, cache_config):
        """Invalidating a specific output removes only that entry."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "out1", 10, "hash1")
            cache.store("recipe1", "step1", "out2", 20, "hash1")
            cache.invalidate("recipe1", "step1", "out1")
            assert cache.lookup("recipe1", "step1", "out1", "hash1") is _CACHE_MISS
            assert cache.lookup("recipe1", "step1", "out2", "hash1") == 20

    def test_invalidate_all_step_outputs(self, cache_config):
        """Invalidating without specifying an output clears all outputs for that step."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "out1", 10, "hash1")
            cache.store("recipe1", "step1", "out2", 20, "hash1")
            cache.store("recipe1", "step2", "out1", 30, "hash1")
            cache.invalidate("recipe1", "step1")
            assert cache.lookup("recipe1", "step1", "out1", "hash1") is _CACHE_MISS
            assert cache.lookup("recipe1", "step1", "out2", "hash1") is _CACHE_MISS
            # step2 should be unaffected
            assert cache.lookup("recipe1", "step2", "out1", "hash1") == 30

    def test_context_manager(self, cache_config):
        """Cache can be used as a context manager."""
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash1")
            assert cache.lookup("recipe1", "step1", "result", "hash1") == 42

    def test_creates_directory(self, tmp_path):
        """Cache creates the directory if it doesn't exist."""
        new_dir = str(tmp_path / "nested" / "cache" / "dir")
        config = CacheConfig(enabled=True, dir=new_dir, name=".test-cache")
        with OutputCache(config) as cache:
            cache.store("recipe1", "step1", "result", 42, "hash1")
        assert os.path.isdir(new_dir)


class TestHashInputs:
    """Tests for _hash_inputs function."""

    def _make_schema(self, is_input=True):
        """Create a simple mock schema object."""

        class MockSchema:
            pass

        s = MockSchema()
        s.is_input = is_input
        return s

    def test_same_inputs_same_hash(self):
        """Identical inputs produce the same hash."""
        params = {"a": 1, "b": "hello"}
        schemas = {"a": self._make_schema(), "b": self._make_schema()}
        h1 = _hash_inputs(params, schemas)
        h2 = _hash_inputs(params, schemas)
        assert h1 == h2

    def test_different_inputs_different_hash(self):
        """Different input values produce different hashes."""
        schemas = {"a": self._make_schema(), "b": self._make_schema()}
        h1 = _hash_inputs({"a": 1, "b": "hello"}, schemas)
        h2 = _hash_inputs({"a": 2, "b": "hello"}, schemas)
        assert h1 != h2

    def test_output_params_ignored(self):
        """Output parameters should not affect the hash."""
        schemas = {
            "a": self._make_schema(is_input=True),
            "out": self._make_schema(is_input=False),
        }
        h1 = _hash_inputs({"a": 1, "out": "x"}, schemas)
        h2 = _hash_inputs({"a": 1, "out": "y"}, schemas)
        assert h1 == h2

    def test_deterministic_ordering(self):
        """Hash is deterministic regardless of dict insertion order."""
        schemas = {"a": self._make_schema(), "b": self._make_schema()}
        from collections import OrderedDict

        p1 = OrderedDict([("a", 1), ("b", 2)])
        p2 = OrderedDict([("b", 2), ("a", 1)])
        assert _hash_inputs(p1, schemas) == _hash_inputs(p2, schemas)

    def test_unset_instances_skipped(self):
        """UNSET(name) instances should be skipped, not just the UNSET class sentinel."""
        from scabha.basetypes import UNSET

        schemas = {"a": self._make_schema(), "b": self._make_schema()}
        h1 = _hash_inputs({"a": 1, "b": UNSET("b")}, schemas)
        h2 = _hash_inputs({"a": 1, "b": UNSET("other")}, schemas)
        h3 = _hash_inputs({"a": 1}, schemas)
        assert h1 == h2, "different UNSET instances should produce the same hash"
        assert h1 == h3, "UNSET values should be equivalent to missing params"


class TestLookupStepOutputs:
    """Tests for the batch lookup/store of step outputs."""

    def _make_schema(self, is_file=False, is_file_list=False):
        class MockSchema:
            pass

        s = MockSchema()
        s.is_file_type = is_file
        s.is_file_list_type = is_file_list
        return s

    def test_lookup_step_outputs(self, cache_config):
        """lookup_step_outputs returns cached non-file outputs."""
        output_schemas = {
            "result_int": self._make_schema(),
            "result_file": self._make_schema(is_file=True),
        }
        with OutputCache(cache_config) as cache:
            cache.store("recipe1", "step1", "result_int", 42, "hash1")
            cache.store("recipe1", "step1", "result_file", "/tmp/file.txt", "hash1")

            cached = cache.lookup_step_outputs("recipe1", "step1", output_schemas, "hash1")

        # only non-file output should be returned
        assert "result_int" in cached
        assert cached["result_int"] == 42
        assert "result_file" not in cached

    def test_store_step_outputs(self, cache_config):
        """store_step_outputs stores only non-file outputs."""

        output_schemas = {
            "result_int": self._make_schema(),
            "result_file": self._make_schema(is_file=True),
        }
        params = {"result_int": 42, "result_file": "/tmp/file.txt"}

        with OutputCache(cache_config) as cache:
            cache.store_step_outputs("recipe1", "step1", params, output_schemas, "hash1")
            # non-file output should be cached
            assert cache.lookup("recipe1", "step1", "result_int", "hash1") == 42
            # file output should NOT be cached
            assert cache.lookup("recipe1", "step1", "result_file", "hash1") is _CACHE_MISS
