from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import heapq
import json
import math
from typing import Sequence


@dataclass
class _Page:
    parent: int | None
    children: set[int] = field(default_factory=set)
    pins: int = 0
    lru_time: float = 0.0
    access_order: int = 0
    residency_generation: int = 0


@dataclass(frozen=True)
class AdmissionPlan:
    version: int
    kind: str
    path: tuple[int, ...]
    additional_pages: int
    evictions: tuple[int, ...]


@dataclass(frozen=True)
class ReuseEvent:
    request_id: int
    time: float
    path: tuple[int, ...]
    generations: tuple[int, ...]


class TaskMainCache:
    """Published trie + independent full-wire buffers, with read-only preflight."""

    def __init__(self, capacity_pages: int) -> None:
        if type(capacity_pages) is not int or capacity_pages < 0:
            raise ValueError("capacity must be a nonnegative integer")
        self.capacity_pages = capacity_pages
        self._pages: dict[int, _Page] = {}
        self._temporary: dict[int, int] = {}
        self._version = 0
        self._access_order = 0
        self._generation = 0
        self.eviction_records: list[tuple[float, int, int]] = []
        self.reuse_events: list[ReuseEvent] = []
        self.peak_memory_pages = 0
        self.peak_resident_pages = 0
        self.peak_temporary_pages = 0
        self._summary_cache_version = -1
        self._summary_cache: dict | None = None

    @property
    def resident_pages(self) -> int:
        return len(self._pages)

    @property
    def temporary_pages(self) -> int:
        return sum(self._temporary.values())

    @property
    def memory_pages(self) -> int:
        return self.resident_pages + self.temporary_pages

    @property
    def pinned_pages(self) -> int:
        return sum(page.pins > 0 for page in self._pages.values())

    def lookup(self, path: Sequence[int]) -> int:
        parent = None
        hit = 0
        for block in path:
            page = self._pages.get(block)
            if page is None or page.parent != parent:
                break
            hit += 1
            parent = block
        return hit

    def _check_path(self, path: tuple[int, ...]) -> None:
        if any(type(block) is not int for block in path) or len(set(path)) != len(path):
            raise ValueError("invalid Prefix path")
        for index, block in enumerate(path):
            if block in self._pages and self._pages[block].parent != (
                path[index - 1] if index else None
            ):
                raise ValueError("conflicting Prefix ancestry")

    def _plan(self, additional: int, protected: set[int], kind: str,
              path: tuple[int, ...] = ()) -> AdmissionPlan | None:
        # Only scratch child counts and a private heap are changed during preflight.
        needed = max(0, self.memory_pages + additional - self.capacity_pages)
        children = {block: len(page.children) for block, page in self._pages.items()}
        heap = [(page.lru_time, page.access_order, block)
                for block, page in self._pages.items()
                if not page.children and not page.pins and block not in protected]
        heapq.heapify(heap)
        evictions: list[int] = []
        while len(evictions) < needed:
            if not heap:
                return None
            _, _, block = heapq.heappop(heap)
            evictions.append(block)
            parent = self._pages[block].parent
            if parent is not None:
                children[parent] -= 1
                page = self._pages[parent]
                if children[parent] == 0 and not page.pins and parent not in protected:
                    heapq.heappush(heap, (page.lru_time, page.access_order, parent))
        return AdmissionPlan(self._version, kind, path, additional, tuple(evictions))

    def plan_temporary(self, wire_pages: int) -> AdmissionPlan | None:
        if type(wire_pages) is not int or wire_pages <= 0:
            raise ValueError("wire_pages must be positive")
        return self._plan(wire_pages, set(), "TEMPORARY")

    def plan_prompt(self, path: Sequence[int]) -> AdmissionPlan | None:
        path = tuple(path)
        self._check_path(path)
        hit = self.lookup(path)
        return self._plan(len(path) - hit, set(path[:hit]), "PROMPT", path)

    def validate_plan(self, plan: AdmissionPlan, kind: str) -> None:
        if plan.version != self._version or plan.kind != kind:
            raise ValueError("stale or incompatible admission plan")

    def _evict(self, plan: AdmissionPlan, now: float) -> None:
        for block in plan.evictions:
            page = self._pages[block]
            if page.pins or page.children:
                raise AssertionError("preflight permitted an illegal eviction")
            if page.parent is not None:
                self._pages[page.parent].children.remove(block)
            del self._pages[block]
            self.eviction_records.append((now, block, page.residency_generation))

    def reserve_temporary(self, transfer_id: int, plan: AdmissionPlan, now: float) -> None:
        self.validate_plan(plan, "TEMPORARY")
        if transfer_id in self._temporary:
            raise ValueError("duplicate Temporary reservation")
        self._time(now)
        self._evict(plan, now)
        self._temporary[transfer_id] = plan.additional_pages
        self._version += 1
        self.assert_invariants()

    def _touch(self, page: _Page, now: float) -> None:
        self._access_order += 1
        page.lru_time = now
        page.access_order = self._access_order

    def _insert(self, path: tuple[int, ...], now: float) -> int:
        inserted = 0
        parent = None
        for block in path:
            if block not in self._pages:
                self._generation += 1
                page = _Page(parent, residency_generation=self._generation)
                self._pages[block] = page
                if parent is not None:
                    self._pages[parent].children.add(block)
                self._touch(page, now)
                inserted += 1
            parent = block
        return inserted

    def publish_prompt(self, plan: AdmissionPlan, now: float) -> int:
        self.validate_plan(plan, "PROMPT")
        self._time(now)
        self._evict(plan, now)
        inserted = self._insert(plan.path, now)
        if inserted != plan.additional_pages:
            raise AssertionError("Prompt admission changed after preflight")
        self._version += 1
        self.assert_invariants()
        return inserted

    def complete_transfer(self, transfer_id: int, path: Sequence[int], now: float) -> int:
        path = tuple(path)
        self._check_path(path)
        self._time(now)
        reserved = self._temporary[transfer_id]
        if reserved != len(path):
            raise ValueError("Temporary must cover the complete wire path")
        del self._temporary[transfer_id]
        inserted = self._insert(path, now)
        self._version += 1
        self.assert_invariants()
        return inserted

    def pin(self, path: Sequence[int]) -> None:
        if self.lookup(path) != len(path):
            raise ValueError("cannot pin an unpublished Prefix")
        for block in path:
            self._pages[block].pins += 1
        self._version += 1

    def unpin(self, path: Sequence[int]) -> None:
        if self.lookup(path) != len(path) or any(self._pages[b].pins <= 0 for b in path):
            raise ValueError("cannot unpin an unprotected Prefix")
        for block in path:
            self._pages[block].pins -= 1
        self._version += 1

    def record_reuse(self, request_id: int, path: Sequence[int], now: float) -> None:
        path = tuple(path)
        self._time(now)
        if self.lookup(path) != len(path):
            raise ValueError("request cannot reuse an unpublished Prefix")
        if not path:
            return
        for block in path:
            self._touch(self._pages[block], now)
        self.reuse_events.append(ReuseEvent(
            request_id, now, path,
            tuple(self._pages[block].residency_generation for block in path),
        ))
        self._version += 1

    @staticmethod
    def _time(now: float) -> None:
        if not math.isfinite(now) or now < 0:
            raise ValueError("Cache time must be finite and nonnegative")

    def assert_invariants(self) -> None:
        if self.memory_pages > self.capacity_pages:
            raise AssertionError("PublishedUnique + Temporary exceeds capacity")
        for block, page in self._pages.items():
            if page.pins < 0:
                raise AssertionError("negative pin count")
            if page.parent is not None:
                if page.parent not in self._pages or block not in self._pages[page.parent].children:
                    raise AssertionError("Prefix closure violated")
            for child in page.children:
                if child not in self._pages or self._pages[child].parent != block:
                    raise AssertionError("invalid child link")
        if any(pages <= 0 for pages in self._temporary.values()):
            raise AssertionError("nonpositive Temporary buffer")
        self.peak_memory_pages = max(self.peak_memory_pages, self.memory_pages)
        self.peak_resident_pages = max(self.peak_resident_pages, self.resident_pages)
        self.peak_temporary_pages = max(self.peak_temporary_pages, self.temporary_pages)

    def page_state(self, block: int) -> dict:
        page = self._pages[block]
        return {"parent": page.parent, "pins": page.pins,
                "lru_time": page.lru_time, "access_order": page.access_order,
                "generation": page.residency_generation}

    def snapshot(self) -> dict:
        return {"capacity_pages": self.capacity_pages,
                "pages": [[block, *self.page_state(block).values()]
                          for block in sorted(self._pages)],
                "temporary": sorted(self._temporary.items()),
                "version": self._version, "access_order": self._access_order,
                "generation": self._generation,
                "evictions": list(self.eviction_records)}

    def summary(self) -> dict:
        if self._summary_cache_version == self._version and self._summary_cache is not None:
            return dict(self._summary_cache)
        digest = hashlib.sha256(json.dumps(
            self.snapshot(), sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest()
        value = {"published_pages": self.resident_pages,
                 "temporary_pages": self.temporary_pages,
                 "memory_pages": self.memory_pages, "pinned_pages": self.pinned_pages,
                 "capacity_pages": self.capacity_pages,
                 "peak_memory_pages": self.peak_memory_pages,
                 "peak_resident_pages": self.peak_resident_pages,
                 "peak_temporary_pages": self.peak_temporary_pages,
                 "state_digest": digest}
        self._summary_cache_version = self._version
        self._summary_cache = value
        return dict(value)
