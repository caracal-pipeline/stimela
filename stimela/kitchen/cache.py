"""Persistent output cache for non-file step outputs.

Uses Python's dbm module to store a key-value cache of step outputs,
enabling reuse of previously computed non-file results across runs.

Keys are formed from the recipe filename, recipe name, step label, and
output name.  Values are pickled Python objects.  An input hash is
stored alongside each cached value so the cache can be invalidated when
inputs change.

See https://github.com/caracal-pipeline/stimela/issues/369
"""

import dbm
import hashlib
import logging
import os
import pickle
from dataclasses import dataclass
from typing import Any, Dict, Optional

from scabha.basetypes import UNSET, Unresolved

log = logging.getLogger(__name__)


# sentinel returned when no cached value is available
_CACHE_MISS = object()


@dataclass
class CacheConfig:
    """Configuration for the output cache."""

    # whether caching is enabled
    enabled: bool = False
    # directory for the cache database; defaults to current directory
    dir: str = "."
    # database filename (without extension; dbm appends its own)
    name: str = ".stimela-cache"


def _cache_db_path(config: CacheConfig) -> str:
    """Return the full path to the cache database file."""
    return os.path.join(config.dir, config.name)


def _make_key(recipe_name: str, step_label: str, output_name: str) -> str:
    """Construct a cache key from recipe name, step label, and output name."""
    return f"{recipe_name}::{step_label}::{output_name}"


def _hash_inputs(params: Dict[str, Any], schemas: Dict[str, Any]) -> str:
    """Compute a deterministic hash over the input parameter values.

    Only input parameters (not outputs) are included in the hash.
    Parameters whose values are UNSET or Unresolved are skipped.
    The hash is used to invalidate cached outputs when inputs change.
    """
    hasher = hashlib.sha256()
    # sort by key for determinism
    for name in sorted(params.keys()):
        schema = schemas.get(name)
        if schema is None or not getattr(schema, "is_input", False):
            continue
        value = params[name]
        if value is UNSET or isinstance(value, Unresolved):
            continue
        # pickle the value for a canonical byte representation
        try:
            pickled = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError, TypeError):
            # if a value can't be pickled, include its repr
            pickled = repr(value).encode("utf-8")
        hasher.update(name.encode("utf-8"))
        hasher.update(pickled)
    return hasher.hexdigest()


class OutputCache:
    """Persistent cache for non-file step outputs.

    Usage::

        cache = OutputCache(config)
        cache.open()
        try:
            # check for cached outputs
            cached = cache.lookup(recipe_name, step_label, output_name, input_hash)
            if cached is not _CACHE_MISS:
                # use cached value
                ...
            else:
                # run step, then store
                cache.store(recipe_name, step_label, output_name, value, input_hash)
        finally:
            cache.close()
    """

    def __init__(self, config: CacheConfig):
        self.config = config
        self._db = None

    def open(self):
        """Open the cache database, creating it if necessary."""
        path = _cache_db_path(self.config)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._db = dbm.open(path, "c")
        log.debug(f"opened output cache at {path}")

    def close(self):
        """Close the cache database."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _value_key(self, recipe_name: str, step_label: str, output_name: str) -> str:
        return _make_key(recipe_name, step_label, output_name)

    def _hash_key(self, recipe_name: str, step_label: str, output_name: str) -> str:
        return _make_key(recipe_name, step_label, output_name) + "::__input_hash__"

    def lookup(
        self,
        recipe_name: str,
        step_label: str,
        output_name: str,
        input_hash: str,
    ) -> Any:
        """Look up a cached output value.

        Returns the cached value if the key exists and the input hash matches,
        otherwise returns ``_CACHE_MISS``.
        """
        if self._db is None:
            return _CACHE_MISS

        vkey = self._value_key(recipe_name, step_label, output_name)
        hkey = self._hash_key(recipe_name, step_label, output_name)

        if vkey not in self._db or hkey not in self._db:
            return _CACHE_MISS

        stored_hash = self._db[hkey].decode("utf-8")
        if stored_hash != input_hash:
            log.debug(f"cache invalidated for {vkey} (input hash mismatch)")
            return _CACHE_MISS

        try:
            value = pickle.loads(self._db[vkey])
            log.debug(f"cache hit for {vkey}")
            return value
        except Exception as exc:
            log.warning(f"failed to unpickle cached value for {vkey}: {exc}")
            return _CACHE_MISS

    def store(
        self,
        recipe_name: str,
        step_label: str,
        output_name: str,
        value: Any,
        input_hash: str,
    ):
        """Store an output value in the cache."""
        if self._db is None:
            return

        vkey = self._value_key(recipe_name, step_label, output_name)
        hkey = self._hash_key(recipe_name, step_label, output_name)

        try:
            self._db[vkey] = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            self._db[hkey] = input_hash.encode("utf-8")
            log.debug(f"cached {vkey}")
        except (pickle.PicklingError, TypeError) as exc:
            log.warning(f"failed to cache {vkey}: {exc}")

    def invalidate(self, recipe_name: str, step_label: str, output_name: Optional[str] = None):
        """Remove cached entries for a given step (or a specific output)."""
        if self._db is None:
            return

        if output_name:
            keys_to_remove = [
                self._value_key(recipe_name, step_label, output_name),
                self._hash_key(recipe_name, step_label, output_name),
            ]
        else:
            # remove all entries for this step
            prefix = _make_key(recipe_name, step_label, "")
            keys_to_remove = [k for k in self._db.keys() if k.decode("utf-8").startswith(prefix)]

        for key in keys_to_remove:
            try:
                if isinstance(key, str):
                    key = key.encode("utf-8")
                del self._db[key]
            except KeyError:
                pass

    def lookup_step_outputs(
        self,
        recipe_name: str,
        step_label: str,
        output_schemas: Dict[str, Any],
        input_hash: str,
    ) -> Dict[str, Any]:
        """Look up all non-file cached outputs for a step.

        Returns a dict of {output_name: cached_value} for outputs that
        have valid cache entries.  Only non-file outputs are considered.
        """
        cached = {}
        for name, schema in output_schemas.items():
            # skip file-type outputs -- those are handled by the existing
            # freshness/existence checks
            if getattr(schema, "is_file_type", False) or getattr(schema, "is_file_list_type", False):
                continue
            value = self.lookup(recipe_name, step_label, name, input_hash)
            if value is not _CACHE_MISS:
                cached[name] = value
        return cached

    def store_step_outputs(
        self,
        recipe_name: str,
        step_label: str,
        params: Dict[str, Any],
        output_schemas: Dict[str, Any],
        input_hash: str,
    ):
        """Store all non-file outputs of a step in the cache."""
        for name, schema in output_schemas.items():
            if getattr(schema, "is_file_type", False) or getattr(schema, "is_file_list_type", False):
                continue
            if name in params:
                value = params[name]
                if value is not UNSET and not isinstance(value, Unresolved):
                    self.store(recipe_name, step_label, name, value, input_hash)
