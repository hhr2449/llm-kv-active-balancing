from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Iterable, Sequence


class CapacityAdmissionError(RuntimeError):
    def __init__(self, *, request_id: int, capacity_pages: int,
                 resident_pages: int, active_private_pages: int,
                 requested_pages: int, evictable_pages: int) -> None:
        self.diagnostic = {
            "error": "capacity_admission_failure",
            "request_id": request_id,
            "capacity_pages": capacity_pages,
            "resident_pages": resident_pages,
            "active_private_pages": active_private_pages,
            "requested_active_private_pages": requested_pages,
            "evictable_leaf_pages": evictable_pages,
            "capacity_admission_failures": 1,
        }
        super().__init__(
            "capacity admission failed: "
            f"request={request_id}, capacity={capacity_pages}, "
            f"resident={resident_pages}, active_private={active_private_pages}, "
            f"requested={requested_pages}, evictable_leaves={evictable_pages}"
        )


@dataclass
class _Node:
    block_id: int | None
    parent: "_Node | None"
    children: dict[int, "_Node"] = field(default_factory=dict)
    pin_count: int = 0
    last_access_ms: float = 0.0
    access_sequence: int = 0
    generation: int = 0


class PrefixCache:
    """Prefix-closed Published Cache with unpinned-leaf LRU eviction."""

    def __init__(self, capacity_pages: int | None = None) -> None:
        if capacity_pages is not None and capacity_pages < 0:
            raise ValueError("capacity_pages must be >= 0 or null")
        self.capacity_pages = capacity_pages
        self._root = _Node(None, None)
        self._resident_pages = 0
        self._active_private: dict[int, int] = {}
        self._active_private_pages = 0
        self._transfer_temporary: dict[int, int] = {}
        self._transfer_temporary_pages = 0
        self._pinned_pages = 0
        self._sequence = 0
        self._leaf_heap: list[tuple[float, int, int, int, _Node]] = []
        self.eviction_count = 0
        self.evicted_pages = 0
        self.accounting_time_ms = 0.0
        self.eviction_timestamps_ms: list[float] = []
        self.eviction_records: list[tuple[float, int]] = []
        self.capacity_admission_failures = 0
        self.peak_resident_pages = 0
        self.peak_active_private_pages = 0
        self.peak_transfer_temporary_pages = 0
        self.peak_memory_used_pages = 0

    @property
    def resident_pages(self) -> int:
        return self._resident_pages

    @property
    def active_private_pages(self) -> int:
        return self._active_private_pages

    @property
    def memory_used_pages(self) -> int:
        return self._resident_pages + self._active_private_pages + self._transfer_temporary_pages

    @property
    def transfer_temporary_pages(self) -> int:
        return self._transfer_temporary_pages

    @property
    def pinned_pages(self) -> int:
        return self._pinned_pages

    def can_admit_temporary(self, pages: int) -> bool:
        """Non-mutating preflight for background-copy admission."""
        if pages < 0:
            return False
        return self.capacity_pages is None or (
            self.pinned_pages + self.active_private_pages
            + self.transfer_temporary_pages + pages <= self.capacity_pages
        )

    def has_free_capacity(self, pages: int) -> bool:
        """Whether admission fits without any Published eviction."""
        return self.capacity_pages is None or self.memory_used_pages + pages <= self.capacity_pages

    def _assert_capacity(self) -> None:
        if self.capacity_pages is not None and self.memory_used_pages > self.capacity_pages:
            raise AssertionError("cache capacity overflow")

    def _update_peaks(self) -> None:
        self.peak_resident_pages = max(self.peak_resident_pages, self.resident_pages)
        self.peak_active_private_pages = max(
            self.peak_active_private_pages, self.active_private_pages
        )
        self.peak_transfer_temporary_pages = max(
            self.peak_transfer_temporary_pages, self.transfer_temporary_pages
        )
        self.peak_memory_used_pages = max(
            self.peak_memory_used_pages, self.memory_used_pages
        )
        self._assert_capacity()

    def _mark_access(self, node: _Node, time_ms: float) -> None:
        self._sequence += 1
        node.last_access_ms = time_ms
        node.access_sequence = self._sequence
        node.generation += 1
        self._push_if_evictable_leaf(node)

    def _push_if_evictable_leaf(self, node: _Node) -> None:
        if node.block_id is not None and not node.children and node.pin_count == 0:
            heapq.heappush(
                self._leaf_heap,
                (node.last_access_ms, node.access_sequence, node.block_id,
                 node.generation, node),
            )

    @staticmethod
    def _is_evictable_leaf(node: _Node) -> bool:
        return node.block_id is not None and not node.children and node.pin_count == 0

    def _pop_lru_leaf(self) -> _Node | None:
        while self._leaf_heap:
            _, _, _, generation, node = heapq.heappop(self._leaf_heap)
            if generation == node.generation and self._is_evictable_leaf(node):
                return node
        return None

    def _evict_one(self) -> bool:
        node = self._pop_lru_leaf()
        if node is None:
            return False
        parent = node.parent
        if parent is None or parent.children.get(node.block_id) is not node:
            raise AssertionError("invalid Prefix Cache ancestry")
        del parent.children[node.block_id]
        self._resident_pages -= 1
        self.eviction_count += 1
        self.evicted_pages += 1
        self.eviction_timestamps_ms.append(self.accounting_time_ms)
        self.eviction_records.append((self.accounting_time_ms, int(node.block_id)))
        node.generation += 1
        self._push_if_evictable_leaf(parent)
        return True

    def _evict_for(self, additional_pages: int) -> bool:
        if additional_pages < 0:
            raise ValueError("additional_pages must be non-negative")
        if self.capacity_pages is None:
            return True
        while self.memory_used_pages + additional_pages > self.capacity_pages:
            if not self._evict_one():
                return False
        return True

    def _count_evictable_leaves(self) -> int:
        count = 0
        stack = list(self._root.children.values())
        while stack:
            node = stack.pop()
            if self._is_evictable_leaf(node):
                count += 1
            else:
                stack.extend(node.children.values())
        return count

    def lookup(self, block_ids: Sequence[int]) -> int:
        """Probe only: lookup never changes recency."""
        node = self._root
        hit_pages = 0
        for block_id in block_ids:
            child = node.children.get(block_id)
            if child is None:
                break
            node = child
            hit_pages += 1
        return hit_pages

    def resident_leaf_block_ids(self) -> tuple[int, ...]:
        """Read-only resident Prefix endpoints for online candidate generation."""
        leaves: list[int] = []
        stack = list(self._root.children.values())
        while stack:
            node = stack.pop()
            if node.children:
                stack.extend(node.children.values())
            else:
                leaves.append(int(node.block_id))
        return tuple(sorted(leaves))

    def pin_and_refresh(self, block_ids: Sequence[int], hit_pages: int,
                        time_ms: float) -> None:
        node = self._root
        for block_id in block_ids[:hit_pages]:
            node = node.children[block_id]
            if node.pin_count == 0:
                self._pinned_pages += 1
            node.pin_count += 1
            node.generation += 1
            self._mark_access(node, time_ms)

    def pin(self, block_ids: Sequence[int], pages: int) -> None:
        node = self._root
        for block_id in block_ids[:pages]:
            node = node.children[block_id]
            if node.pin_count == 0:
                self._pinned_pages += 1
            node.pin_count += 1
            node.generation += 1

    def unpin(self, block_ids: Sequence[int], hit_pages: int) -> None:
        node = self._root
        for block_id in block_ids[:hit_pages]:
            node = node.children[block_id]
            if node.pin_count <= 0:
                raise AssertionError("unpin of an unpinned block")
            node.pin_count -= 1
            if node.pin_count == 0:
                self._pinned_pages -= 1
            node.generation += 1
            self._push_if_evictable_leaf(node)

    def reserve_active_private(self, request_id: int, pages: int) -> None:
        if request_id in self._active_private:
            raise AssertionError("request already has ActivePrivate reservation")
        impossible = (
            self.capacity_pages is not None
            and self.pinned_pages + self.active_private_pages
            + self.transfer_temporary_pages + pages
            > self.capacity_pages
        )
        admitted = False if impossible else self._evict_for(pages)
        if not admitted:
            self.capacity_admission_failures += 1
            if self.capacity_pages is None:
                raise AssertionError("infinite cache admission unexpectedly failed")
            raise CapacityAdmissionError(
                request_id=request_id,
                capacity_pages=self.capacity_pages,
                resident_pages=self.resident_pages,
                active_private_pages=self.active_private_pages,
                requested_pages=pages,
                evictable_pages=self._count_evictable_leaves(),
            )
        self._active_private[request_id] = pages
        self._active_private_pages += pages
        self._update_peaks()

    def reserve_transfer_temporary(self, transfer_id: int, pages: int) -> None:
        if transfer_id in self._transfer_temporary:
            raise AssertionError("transfer already has temporary reservation")
        impossible = (
            self.capacity_pages is not None
            and self.pinned_pages + self.active_private_pages
            + self.transfer_temporary_pages + pages > self.capacity_pages
        )
        admitted = False if impossible else self._evict_for(pages)
        if not admitted:
            self.capacity_admission_failures += 1
            if self.capacity_pages is None:
                raise AssertionError("infinite transfer admission failed")
            raise CapacityAdmissionError(
                request_id=transfer_id, capacity_pages=self.capacity_pages,
                resident_pages=self.resident_pages,
                active_private_pages=self.active_private_pages + self.transfer_temporary_pages,
                requested_pages=pages, evictable_pages=self._count_evictable_leaves(),
            )
        self._transfer_temporary[transfer_id] = pages
        self._transfer_temporary_pages += pages
        self._update_peaks()

    def release_transfer_temporary(self, transfer_id: int) -> None:
        pages = self._transfer_temporary.pop(transfer_id)
        self._transfer_temporary_pages -= pages
        self._assert_capacity()

    def commit_transfer_temporary(self, transfer_id: int,
                                  block_ids: Sequence[int], time_ms: float) -> int:
        if transfer_id not in self._transfer_temporary:
            raise AssertionError("transfer has no temporary reservation")
        reserved = self._transfer_temporary.pop(transfer_id)
        missing = len(block_ids) - self.lookup(block_ids)
        if missing > reserved:
            raise AssertionError("transfer commit exceeds temporary reservation")
        node = self._root
        inserted = 0
        for block_id in block_ids:
            child = node.children.get(block_id)
            if child is None:
                child = _Node(block_id, node)
                node.children[block_id] = child
                self._resident_pages += 1
                self._transfer_temporary_pages -= 1
                inserted += 1
                self._mark_access(child, time_ms)
                self._assert_capacity()
            node = child
        self._transfer_temporary_pages -= reserved - inserted
        self._update_peaks()
        return inserted

    def commit_active(self, request_id: int, block_ids: Sequence[int],
                      time_ms: float) -> int:
        if request_id not in self._active_private:
            raise AssertionError("request has no ActivePrivate reservation")
        reserved = self._active_private.pop(request_id)
        missing = len(block_ids) - self.lookup(block_ids)
        if missing > reserved:
            raise AssertionError("commit would insert more pages than were reserved")
        node = self._root
        inserted = 0
        for block_id in block_ids:
            child = node.children.get(block_id)
            if child is None:
                child = _Node(block_id, node)
                node.children[block_id] = child
                self._resident_pages += 1
                self._active_private_pages -= 1
                inserted += 1
                self._mark_access(child, time_ms)
                self._assert_capacity()
            node = child
        self._active_private_pages -= reserved - inserted
        self._update_peaks()
        return inserted

    def publish(self, block_ids: Iterable[int], time_ms: float = 0.0) -> int:
        """Capacity-safe, idempotent direct publish helper for preload/tests."""
        path = tuple(block_ids)
        missing = len(path) - self.lookup(path)
        request_id = -1
        while request_id in self._active_private:
            request_id -= 1
        self.reserve_active_private(request_id, missing)
        return self.commit_active(request_id, path, time_ms)

    def release_leaf_exclusive_suffix(
        self, block_ids: Sequence[int], protected_transfer_blocks: Iterable[int] = ()
    ) -> dict[str, int]:
        """Release only the safely removable leaf suffix of a Published path."""
        node = self._root
        path: list[_Node] = []
        for block_id in block_ids:
            child = node.children.get(block_id)
            if child is None:
                break
            path.append(child)
            node = child
        protected = set(protected_transfer_blocks)
        freed = 0
        blocked_shared = blocked_pinned = blocked_active_or_transfer = 0
        for node in reversed(path):
            if node.children:
                blocked_shared = 1
                break
            if node.pin_count > 0:
                if node.block_id in protected:
                    blocked_active_or_transfer = 1
                else:
                    blocked_pinned = 1
                break
            parent = node.parent
            if parent is None or parent.children.get(node.block_id) is not node:
                raise AssertionError("invalid Prefix Cache ancestry during safe release")
            del parent.children[node.block_id]
            self._resident_pages -= 1
            node.generation += 1
            self.eviction_records.append((self.accounting_time_ms, int(node.block_id)))
            self._push_if_evictable_leaf(parent)
            freed += 1
        self._assert_capacity()
        return {
            "pages_freed": freed,
            "blocked_shared": blocked_shared,
            "blocked_pinned": blocked_pinned,
            "blocked_active_or_transfer": blocked_active_or_transfer,
        }

    def is_pinned(self, block_ids: Sequence[int]) -> bool:
        node = self._root
        for block_id in block_ids:
            child = node.children.get(block_id)
            if child is None:
                return False
            node = child
        return node.pin_count > 0

    def diagnostic_snapshot(self) -> tuple:
        nodes = []
        stack = list(self._root.children.values())
        while stack:
            node = stack.pop()
            nodes.append((node.block_id, node.parent.block_id if node.parent else None,
                          node.pin_count, node.last_access_ms, node.access_sequence))
            stack.extend(node.children.values())
        return (tuple(sorted(nodes)), tuple(sorted(self._active_private.items())),
                tuple(sorted(self._transfer_temporary.items())), self._sequence,
                self._resident_pages, self._pinned_pages)
