from collections import deque
from dataclasses import replace
import math

from ..config import LOAD_WINDOW_MS
from ..load import CommittedTokenLoad, LoadEntry


class ObservationalLoad(CommittedTokenLoad):
    """Queries never prune or move the clock; only real commits prune storage."""

    def vector(self, now):
        if not math.isfinite(now) or now < 0 or now < self._last_time:
            raise ValueError('Load query precedes last real commit')
        return tuple(sum(e.miss_tokens for e in history
                         if now-LOAD_WINDOW_MS < e.account_time <= now)
                     for history in self._histories)

    def commit(self, request_id, pod_id, miss_tokens, now):
        if request_id in self.entries:
            raise ValueError('request already has a final Load assignment')
        if not 0 <= pod_id < len(self._histories) or type(miss_tokens) is not int or miss_tokens < 0:
            raise ValueError('invalid final assignment')
        self.vector(now)
        self._last_time = now
        for p, history in enumerate(self._histories):
            while history and history[0].account_time <= now-LOAD_WINDOW_MS:
                self._totals[p] -= history.popleft().miss_tokens
        entry = LoadEntry(request_id,pod_id,now,miss_tokens)
        self.entries[request_id]=entry
        self._histories[pod_id].append(entry)
        self._totals[pod_id]+=miss_tokens

    def reconcile_hit(self, request_id, miss_tokens):
        """Ready-time actual hit correction; keep the original assignment timestamp."""
        old = self.entries[request_id]
        new = replace(old, miss_tokens=miss_tokens)
        self.entries[request_id] = new
        history = self._histories[old.pod_id]
        for i, entry in enumerate(history):
            if entry.request_id == request_id:
                history[i] = new
                self._totals[old.pod_id] += miss_tokens-old.miss_tokens
                break
