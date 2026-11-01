"""Exact cache semantics with incremental invariants and exact JSON digest chunks.

All policy-visible data and SHA256 snapshot bytes match DiagnosticCache. Full
invariants are checked at checkpoint/finalization; mutation checks cover the
changed nodes and their parents/children (inductive closure validation).
"""
import hashlib
import json
import math

from ..cache import AdmissionPlan, TaskMainCache
from ..stage0.cache import DiagnosticCache


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class IndexedCache(DiagnosticCache):
    def __init__(self, capacity, pod, universe, chunks):
        super().__init__(capacity)
        self.pod, self.universe, self.chunks = pod, universe, chunks
        self._pins_count = 0
        self._changed = set()
        self._page_chunks = {}
        self._chunk_bytes = {}
        self._eviction_chunks = []
        self._encoded_evictions = 0
        self._memo_version = -1
        self._memo = None

    @property
    def pinned_pages(self):
        return self._pins_count

    def _plan(self, additional, protected, kind, path=()):
        if self.memory_pages + additional <= self.capacity_pages:
            return AdmissionPlan(self._version, kind, path, additional, ())
        return super()._plan(additional, protected, kind, path)

    def _refresh_bytes(self, blocks):
        dirty = set()
        for b in blocks:
            # Synthetic inputs can publish blocks outside the supplied metadata.
            chunk = self.chunks.get(b, b)
            rows = self._page_chunks.setdefault(chunk, {})
            if b in self._pages:
                rows[b] = encode([b, *self.page_state(b).values()])
            else:
                rows.pop(b, None)
            dirty.add(chunk)
        for chunk in dirty:
            rows = self._page_chunks[chunk]
            self._chunk_bytes[chunk] = b','.join(rows[b] for b in sorted(rows))

    def _insert(self, path, now):
        missing = [b for b in path if b not in self._pages]
        self._changed.update(path)
        result = super()._insert(path, now)
        self._refresh_bytes(path)
        for b in missing:
            self.universe.resident(self.pod, b, True)
        return result

    def _evict(self, plan, now):
        for b in plan.evictions:
            parent = self._pages[b].parent
            if parent is not None:
                self._changed.add(parent)
        self._changed.update(plan.evictions)
        super()._evict(plan, now)
        self._refresh_bytes(plan.evictions)
        for b in plan.evictions:
            self.universe.resident(self.pod, b, False)
        if len(self.eviction_records) != self._encoded_evictions:
            first = self._encoded_evictions // 256
            self._eviction_chunks[first:] = [b','.join(encode(r) for r in self.eviction_records[i:i+256])
                for i in range(first * 256, len(self.eviction_records), 256)]
            self._encoded_evictions = len(self.eviction_records)

    def pin(self, path):
        newly = sum(self._pages[b].pins == 0 for b in path)
        super().pin(path)
        self._pins_count += newly
        self._changed.update(path)
        self._refresh_bytes(path)
        self._memoize()

    def unpin(self, path):
        released = sum(self._pages[b].pins == 1 for b in path)
        super().unpin(path)
        self._pins_count -= released
        self._changed.update(path)
        self._refresh_bytes(path)
        self._memoize()

    def reserve_temporary(self, transfer_id, plan, now):
        value = super().reserve_temporary(transfer_id, plan, now)
        self._memoize()
        return value

    def complete_transfer(self, transfer_id, path, now):
        value = super().complete_transfer(transfer_id, path, now)
        self._memoize()
        return value

    def _memoize(self):
        self._memo = self._summary()
        self._memo_version = self._version

    def record_reuse(self, request_id, path, now):
        super().record_reuse(request_id, path, now)
        self._refresh_bytes(path)

    def assert_invariants(self):
        if self.memory_pages > self.capacity_pages or self._pins_count < 0:
            raise AssertionError('invalid capacity or pin accounting')
        for block in self._changed:
            page = self._pages.get(block)
            if page is None:
                continue
            assert page.pins >= 0
            if page.parent is not None:
                assert block in self._pages[page.parent].children
            for child in page.children:
                assert self._pages[child].parent == block
        self._changed.clear()
        assert all(p > 0 for p in self._temporary.values())
        self.peak_memory_pages = max(self.peak_memory_pages, self.memory_pages)
        self.peak_resident_pages = max(self.peak_resident_pages, self.resident_pages)
        self.peak_temporary_pages = max(self.peak_temporary_pages, self.temporary_pages)

    def full_check(self):
        TaskMainCache.assert_invariants(self)
        assert self.pinned_pages == sum(p.pins > 0 for p in self._pages.values())
        assert self.summary()['state_digest'] == hashlib.sha256(encode(self.snapshot())).hexdigest()

    def summary(self):
        if self._memo_version == self._version:
            return dict(self._memo)
        return self._summary()

    def _summary(self):
        capacity = None if math.isinf(self.capacity_pages) else self.capacity_pages
        # The sorted-key canonical snapshot has exactly these eight fields.
        h = hashlib.sha256()
        h.update(b'{"access_order":' + encode(self._access_order) + b',"capacity_pages":' + encode(capacity))
        h.update(b',"evictions":[')
        h.update(b','.join(self._eviction_chunks))
        h.update(b'],"generation":' + encode(self._generation) + b',"pages":[')
        h.update(b','.join(self._chunk_bytes[k] for k in sorted(self._chunk_bytes) if self._chunk_bytes[k]))
        h.update(b'],"temporary":' + encode(sorted(self._temporary.items())) + b',"version":' + encode(self._version) + b'}')
        return dict(published_pages=self.resident_pages, temporary_pages=self.temporary_pages,
                    memory_pages=self.memory_pages, pinned_pages=self.pinned_pages, capacity_pages=capacity,
                    peak_memory_pages=self.peak_memory_pages, peak_resident_pages=self.peak_resident_pages,
                    peak_temporary_pages=self.peak_temporary_pages, state_digest=h.hexdigest())
