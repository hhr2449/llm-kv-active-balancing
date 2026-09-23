"""Lossless, bounded-memory candidate diagnostics with immutable gzip segments."""
import gzip
import hashlib
from pathlib import Path
import pickle
import uuid

from ..stage0.engine import DiagnosticController


class DecisionLedger:
    def __init__(self, directory):
        self.directory = str(directory)
        Path(directory).mkdir(parents=True, exist_ok=True)
        self.segments = []
        self.pending = []
        self.count = 0
        self.segment_sha256 = {}

    def extend(self, rows):
        # These objects are still updated by execute(); flush only afterward.
        self.pending.extend(rows)
        self.count += len(rows)

    def flush(self, force=False):
        if self.pending and (force or len(self.pending) >= 20000):
            path = Path(self.directory) / (uuid.uuid4().hex + '.pickle.gz')
            with gzip.open(path, 'wb', compresslevel=1) as handle:
                pickle.dump(self.pending, handle, protocol=5)
            self.segments.append(str(path))
            self.segment_sha256[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.pending = []

    def __len__(self):
        return self.count

    def __iter__(self):
        for path in self.segments:
            with gzip.open(path, 'rb') as handle:
                yield from pickle.load(handle)
        yield from self.pending


class StreamingController(DiagnosticController):
    def execute(self, opportunity):
        value = super().execute(opportunity)
        self.decisions.flush()
        return value
