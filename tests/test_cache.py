"""AsyncKeyedCache: a failed build must not evict a newer entry."""

import asyncio

import pytest

from kynomesh.client._cache import AsyncKeyedCache


async def test_build_kwargs_reach_the_builder_on_first_use_only():
    calls = []

    async def build(key, **kwargs):
        calls.append((key, kwargs))
        return object()

    cache = AsyncKeyedCache(build)

    first = await cache.get("a", headers={"X": "1"})
    second = await cache.get("a", headers={"X": "2"})

    assert first is second
    assert calls == [("a", {"headers": {"X": "1"}})]


async def test_a_failed_build_is_evicted_so_the_next_call_retries():
    attempts = []

    async def build(key):
        attempts.append(key)
        if len(attempts) == 1:
            raise RuntimeError("boom")
        return object()

    cache = AsyncKeyedCache(build)

    with pytest.raises(RuntimeError):
        await cache.get("a")
    await cache.get("a")

    assert attempts == ["a", "a"]


async def test_a_stale_failure_does_not_evict_a_newer_entry():
    release_stale = asyncio.Event()
    newer = object()
    order = []

    async def build(key, *, stale: bool = False):
        if stale:
            order.append("stale-start")
            await release_stale.wait()
            raise RuntimeError("stale build failed")
        order.append("newer-built")
        return newer

    cache = AsyncKeyedCache(build)

    stale_task = asyncio.ensure_future(cache.get("a", stale=True))
    while "stale-start" not in order:
        await asyncio.sleep(0)
    cache.forget("a")  # the caller drops the in-flight entry...
    assert await cache.get("a") is newer  # ...and another caller builds a fresh one
    release_stale.set()
    with pytest.raises(RuntimeError):
        await stale_task

    assert await cache.get("a") is newer
    assert order == ["stale-start", "newer-built"]
