"""PY04-ORC-001: exhaustive ideal-rank distributions, not fixed-key equality.

Uniform finite ideal ranks are injected at the existing private entropy seam.
Production terminal_permutation still performs word layout, native lexsort,
whole-batch rejection, and its eight-attempt limit. Exact conditional outcome
frequencies are compared with an independent Fisher-Yates choice-tree oracle.
These finite exhaustive cases do not prove the production CSPRNG's security.
"""
from collections import Counter
from fractions import Fraction
from itertools import permutations, product
from math import comb, factorial
import struct

import pytest

from readmixer_py import shuffle
from readmixer_py.errors import RankCollisionError


_PATH = (7, 13)


def fisher_yates_distribution(size):
    """Enumerate every uniform choice in a descending Fisher-Yates loop."""
    distribution = Counter()
    for choices in product(*(range(position + 1) for position in range(size - 1, 0, -1))):
        values = list(range(size))
        for position, selected in zip(range(size - 1, 0, -1), choices):
            values[position], values[selected] = values[selected], values[position]
        distribution[tuple(values)] += 1
    return distribution


def assert_same_conditional_distribution(observed, ideal):
    assert set(observed) == set(ideal)
    observed_total, ideal_total = sum(observed.values()), sum(ideal.values())
    assert {order: Fraction(count, observed_total) for order, count in observed.items()} == {
        order: Fraction(count, ideal_total) for order, count in ideal.items()}


class IdealRankEntropy:
    """Supply entire ideal rank batches; repeat the last batch if exhausted.

    The distinct integer universe is embedded in 128-bit (high, low) pairs with
    equal high words and low-word values straddling the unsigned signed boundary.
    No implementation rank comparison is used to define the oracle ordering.
    """
    def __init__(self, monkeypatch, size):
        self.size = size
        self.calls = []
        self.batches = ()
        monkeypatch.setattr(shuffle, '_fill_rank_bytes', self.fill)

    def load(self, *batches):
        assert batches and all(len(batch) == self.size for batch in batches)
        self.batches = batches
        self.calls.clear()

    def rank_key(self, path, size, attempt):
        assert path == _PATH and size == self.size
        assert attempt == len(self.calls) and 0 <= attempt <= 7
        self.calls.append(attempt)
        return attempt.to_bytes(32, 'big')

    def fill(self, destination, key, **_kwargs):
        attempt = int.from_bytes(key, 'big')
        assert attempt == self.calls[-1]
        batch = self.batches[min(attempt, len(self.batches) - 1)]
        assert len(destination) == 16 * self.size
        high = [rank // 2 for rank in batch]
        low = [(rank % 2) << 63 for rank in batch]
        destination[:] = struct.pack('<' + 'Q' * (2 * self.size), *(high + low))

    def run(self):
        actual = tuple(map(int, shuffle.terminal_permutation(
            self, _PATH, self.size, _collision_window=2)))
        assert tuple(sorted(actual)) == tuple(range(self.size))
        return actual


@pytest.mark.parametrize('size', range(5))
def test_PY04_ORC_001_exhaustive_distinct_rank_distribution_matches_fisher_yates(monkeypatch, size):
    oracle = fisher_yates_distribution(size)
    # Independently establish that the swap-choice oracle enumerates all n!
    # possible bijections once, including the empty and singleton domains.
    assert oracle == Counter(permutations(range(size)))
    assert len(oracle) == factorial(size)
    universe = size + 1
    entropy = IdealRankEntropy(monkeypatch, size)
    observed = Counter()
    rejected = 0
    for ranks in product(range(universe), repeat=size):
        entropy.load(ranks)
        if len(set(ranks)) == size:
            result = entropy.run()
            assert result == tuple(sorted(range(size), key=lambda index: ranks[index]))
            assert entropy.calls == [0]
            observed[result] += 1
        else:
            # Constant colliding batches cannot be silently tie-broken or have
            # just one pair kept/re-ranked. Every full attempt must be rejected.
            with pytest.raises(RankCollisionError):
                entropy.run()
            assert entropy.calls == list(range(8))
            rejected += 1
    assert rejected == universe ** size - factorial(size) * comb(universe, size)
    assert set(observed.values()) == {comb(universe, size)}
    assert_same_conditional_distribution(observed, oracle)


@pytest.mark.parametrize('size', (2, 3))
def test_PY04_ORC_001_exhaustive_two_batch_rejection_preserves_fisher_yates_distribution(monkeypatch, size):
    """Enumerate all first/second ideal draws, including both colliding.

    If neither draw succeeds, repeating the second collision exhausts attempts
    2..7. Thus this test conditions on success within its first two ideal draws;
    it does not pretend these forced suffixes are independent entropy draws.
    """
    universe = size + 1
    batches = tuple(product(range(universe), repeat=size))
    entropy = IdealRankEntropy(monkeypatch, size)
    observed = Counter()
    retried = rejected = 0
    for first, second in product(batches, repeat=2):
        entropy.load(first, second)
        accepted = first if len(set(first)) == size else second
        if len(set(accepted)) != size:
            with pytest.raises(RankCollisionError):
                entropy.run()
            assert entropy.calls == list(range(8))
            rejected += 1
            continue
        result = entropy.run()
        assert result == tuple(sorted(range(size), key=lambda index: accepted[index]))
        if len(set(first)) == size:
            assert entropy.calls == [0]
        else:
            assert entropy.calls == [0, 1]
            retried += 1
        observed[result] += 1
    distinct = factorial(size) * comb(universe, size)
    colliding = universe ** size - distinct
    assert retried == colliding * distinct
    assert rejected == colliding ** 2
    assert sum(observed.values()) + rejected == universe ** (2 * size)
    assert_same_conditional_distribution(observed, fisher_yates_distribution(size))


def test_PY04_ORC_001_exhaustive_final_retry_preserves_fisher_yates_distribution(monkeypatch):
    size, universe = 4, 5
    entropy = IdealRankEntropy(monkeypatch, size)
    # Equal ranks at sorted positions 1 and 2 cross the two-entry collision
    # checking window. Seven rejected whole batches precede each ideal draw.
    collision = (0, 1, 1, 2)
    observed = Counter()
    rejected = 0
    for final in product(range(universe), repeat=size):
        entropy.load(*([collision] * 7), final)
        if len(set(final)) != size:
            with pytest.raises(RankCollisionError):
                entropy.run()
            rejected += 1
        else:
            result = entropy.run()
            assert result == tuple(sorted(range(size), key=lambda index: final[index]))
            observed[result] += 1
        assert entropy.calls == list(range(8))
    assert rejected == universe ** size - factorial(size) * comb(universe, size)
    assert set(observed.values()) == {comb(universe, size)}
    assert_same_conditional_distribution(observed, fisher_yates_distribution(size))
