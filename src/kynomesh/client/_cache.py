"""Lazy, per-process cache of peer clients.

Concurrency scope is explicitly single-event-loop asyncio, not
cross-thread or cross-event-loop. A cached client returned by
peer_client() must not be shared across OS threads or multiple event
loops.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

_T = TypeVar("_T")


@dataclass
class _CacheEntry(Generic[_T]):
    """Holds the lazily-built value for one key, plus the lock guarding
    its construction so concurrent first-callers await a single build
    instead of racing.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    built: bool = False
    value: _T | None = None
    error: Exception | None = None


class AsyncKeyedCache(Generic[_T]):
    """A lazy, per-key async value cache for a single event loop.

    The first call for a given key runs `builder(key)` and caches the
    result; every later call for that key returns the cached value.
    Concurrent first-use of the same key is safe: only one caller runs
    the builder, and the rest await its result.

    Not safe to share across threads or event loops.
    """

    def __init__(self, builder: Callable[[str], Awaitable[_T]]) -> None:
        self._builder = builder
        self._entries_lock = asyncio.Lock()
        self._entries: dict[str, _CacheEntry[_T]] = {}

    async def get(self, key: str) -> _T:
        async with self._entries_lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _CacheEntry()
                self._entries[key] = entry

        async with entry.lock:
            if not entry.built:
                try:
                    entry.value = await self._builder(key)
                except Exception as e:  # noqa: BLE001
                    entry.error = e
                    # Don't let a failed build permanently poison the
                    # cache for this key: evict now so the next call
                    # gets a fresh attempt instead of the same error
                    # forever.
                    self.forget(key)
                    raise
                entry.built = True

        assert entry.value is not None
        return entry.value

    def forget(self, key: str) -> None:
        """Drops the cached entry for key, if any. Safe to call for a
        key with no cached entry.
        """
        self._entries.pop(key, None)

    def reset(self) -> None:
        """Drops every cached entry. Test-only; not exposed as a public API."""
        self._entries = {}
