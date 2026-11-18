"""Deterministic random helpers.

Only ``random.Random.random()`` is used as the primitive, because Python guarantees that
method is reproducible across versions. Other ``random`` helpers may change between releases.
"""

from __future__ import annotations

import bisect
import math
import random
from collections.abc import Sequence


class Rng:
    def __init__(self, seed: int) -> None:
        self._r = random.Random(seed)

    def random(self) -> float:
        return self._r.random()

    def uniform(self, a: float, b: float) -> float:
        return a + (b - a) * self.random()

    def randint(self, a: int, b: int) -> int:
        """Inclusive on both ends."""
        return a + int(self.random() * (b - a + 1))

    def chance(self, p: float) -> bool:
        return self.random() < p

    def choice(self, seq: Sequence):
        return seq[int(self.random() * len(seq))]

    def normal(self) -> float:
        u1 = 1.0 - self.random()
        u2 = self.random()
        return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)

    def pick_weighted(self, cumulative: Sequence[float]) -> int:
        """Index chosen from a cumulative-weight list (last value is the total)."""
        return min(
            bisect.bisect_right(cumulative, self.random() * cumulative[-1]), len(cumulative) - 1
        )


def cumulative(weights: Sequence[float]) -> list[float]:
    total = 0.0
    out = []
    for w in weights:
        total += w
        out.append(total)
    return out
