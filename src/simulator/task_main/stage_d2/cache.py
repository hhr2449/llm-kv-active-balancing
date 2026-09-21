from __future__ import annotations

from collections import Counter
import heapq

from ..cache import AdmissionPlan, TaskMainCache


class StageD2Cache(TaskMainCache):
    """Frozen Cache plus a separate, non-LRU MOVE safe-release ledger."""

    def __init__(self, capacity_pages):
        super().__init__(capacity_pages)
        self._committed_protections = Counter()
        self.move_source_release_records = []
        self._temporary_plan_version = -1
        self._temporary_eviction_order = ()

    def _build_temporary_eviction_order(self):
        """Build the exact leaf-LRU eviction sequence once for this Cache version.

        Full-wire Temporary admission has no path-specific protected set. Every
        preflight against an unchanged Cache therefore consumes a prefix of the
        same deterministic sequence. Reusing this sequence changes neither the
        selected victims nor the immutable AdmissionPlan returned to callers.
        """
        children = {block: len(page.children) for block, page in self._pages.items()}
        heap = [(page.lru_time, page.access_order, block)
                for block, page in self._pages.items()
                if not page.children and not page.pins]
        heapq.heapify(heap)
        order = []
        while heap:
            _, _, block = heapq.heappop(heap)
            order.append(block)
            parent = self._pages[block].parent
            if parent is not None:
                children[parent] -= 1
                page = self._pages[parent]
                if children[parent] == 0 and not page.pins:
                    heapq.heappush(heap, (page.lru_time, page.access_order, parent))
        self._temporary_plan_version = self._version
        self._temporary_eviction_order = tuple(order)

    def plan_temporary(self, wire_pages):
        if type(wire_pages) is not int or wire_pages <= 0:
            raise ValueError("wire_pages must be positive")
        if self._temporary_plan_version != self._version:
            self._build_temporary_eviction_order()
        needed = max(0, self.memory_pages + wire_pages - self.capacity_pages)
        if needed > len(self._temporary_eviction_order):
            return None
        return AdmissionPlan(self._version, "TEMPORARY", (), wire_pages,
                             self._temporary_eviction_order[:needed])

    @property
    def committed_protected_pages(self):
        return sum(value > 0 for value in self._committed_protections.values())

    def protect_committed(self, path):
        for block in path:
            self._committed_protections[block] += 1
        self._version += 1

    def unprotect_committed(self, path):
        for block in path:
            if self._committed_protections[block] <= 0:
                raise ValueError("committed protection underflow")
            self._committed_protections[block] -= 1
            if not self._committed_protections[block]:
                del self._committed_protections[block]
        self._version += 1

    def source_generations(self, path):
        path = tuple(path)
        if self.lookup(path) != len(path):
            raise ValueError("MOVE source must own the complete chain")
        return tuple(self._pages[block].residency_generation for block in path)

    def _assess_suffix(self, path, generations, *, mutate=False, now=None):
        path, generations = tuple(path), tuple(generations)
        if len(path) != len(generations) or not path:
            raise ValueError("MOVE generation vector must match a nonempty chain")
        released = []
        released_set = set()
        reason = "FULL_RELEASE"
        for block, expected_generation in zip(reversed(path), reversed(generations)):
            page = self._pages.get(block)
            if page is None:
                reason = "NOT_RESIDENT"
                break
            if page.residency_generation != expected_generation:
                reason = "GENERATION_CHANGED"
                break
            if self._committed_protections.get(block, 0):
                reason = "COMMITTED_PROTECTION"
                break
            if page.pins:
                reason = "ACTIVE_PIN"
                break
            if any(child not in released_set for child in page.children):
                reason = "SHARED_OR_NONLEAF"
                break
            released.append(block)
            released_set.add(block)
            if mutate:
                if page.parent is not None:
                    self._pages[page.parent].children.remove(block)
                del self._pages[block]
        if mutate and released:
            self._version += 1
            self.assert_invariants()
        status = ("FULL_RELEASE" if len(released) == len(path) else
                  "ZERO_RELEASE" if not released else "PARTIAL_RELEASE")
        if status == "FULL_RELEASE":
            reason = "FULL_RELEASE"
        return {
            "released_blocks_deep_to_root": tuple(released),
            "actual_source_released_pages": len(released),
            "release_fraction": len(released)/len(path),
            "release_status": status,
            "release_stop_reason": reason,
            "ready_time": now,
        }

    def predicted_releasable_pages(self, path, generations):
        return self._assess_suffix(path, generations)["actual_source_released_pages"]

    def safe_release_suffix(self, path, generations, now):
        result = self._assess_suffix(path, generations, mutate=True, now=now)
        self.move_source_release_records.append(dict(result, chain_id=tuple(path),
                                                     source_generation_vector=tuple(generations)))
        return result

    def cancel_temporary(self, transfer_id):
        if transfer_id in self._temporary:
            del self._temporary[transfer_id]
            self._version += 1
            self.assert_invariants()

    def snapshot(self):
        value = super().snapshot()
        value["committed_protections"] = sorted(self._committed_protections.items())
        value["move_source_release_records"] = list(self.move_source_release_records)
        return value

    def assert_invariants(self):
        super().assert_invariants()
        if any(value <= 0 for value in self._committed_protections.values()):
            raise AssertionError("invalid committed protection count")
