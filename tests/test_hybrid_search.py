"""Tests for the RRF fusion math in src/rag/hybrid_search.py.

These build a HybridSearcher WITHOUT calling __init__, so the embedding
model and Chroma/BM25 index never load - only the fusion formula itself
(score = sum over rankings of 1 / (rrf_k + rank + 1)) is under test. This
keeps the suite fast and network-free, which matters for re-running it
during grading.
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "rag"))
from hybrid_search import HybridSearcher  # noqa: E402


def _bare_searcher():
    searcher = HybridSearcher.__new__(HybridSearcher)
    searcher._bm25_chunk_ids = ["a", "b", "c", "d"]
    searcher._bm25_texts = ["text a", "text b", "text c", "text d"]
    searcher._bm25_metadatas = [
        {"doc_title": "Doc", "section_title": f"Section {cid}"} for cid in ["a", "b", "c", "d"]
    ]
    return searcher


def test_rrf_formula_matches_known_values():
    searcher = _bare_searcher()

    # Both rankings agree exactly: a first, b second.
    with patch.object(searcher, "_dense_ranking", return_value=["a", "b"]), \
         patch.object(searcher, "_sparse_ranking", return_value=["a", "b"]):
        results = searcher.search("query", top_k=2, rrf_k=60)

    expected_a = 2 * (1.0 / (60 + 1))  # rank 0 in both lists
    expected_b = 2 * (1.0 / (60 + 2))  # rank 1 in both lists

    assert results[0].chunk_id == "a"
    assert abs(results[0].fused_score - expected_a) < 1e-9
    assert results[1].chunk_id == "b"
    assert abs(results[1].fused_score - expected_b) < 1e-9


def test_item_ranked_high_in_both_lists_beats_item_in_only_one():
    searcher = _bare_searcher()

    # "a" appears near the top of BOTH rankings; "c" only appears in dense,
    # at a worse rank. RRF should place "a" strictly above "c".
    with patch.object(searcher, "_dense_ranking", return_value=["a", "c"]), \
         patch.object(searcher, "_sparse_ranking", return_value=["a", "d"]):
        results = searcher.search("query", top_k=3, rrf_k=60)

    ids_in_order = [r.chunk_id for r in results]
    assert ids_in_order[0] == "a"
    assert results[0].fused_score > results[-1].fused_score


def test_item_found_in_only_one_ranking_is_still_included():
    searcher = _bare_searcher()

    with patch.object(searcher, "_dense_ranking", return_value=["a"]), \
         patch.object(searcher, "_sparse_ranking", return_value=["d"]):
        results = searcher.search("query", top_k=2)

    assert {r.chunk_id for r in results} == {"a", "d"}


def test_citation_property_combines_doc_and_section_title():
    searcher = _bare_searcher()

    with patch.object(searcher, "_dense_ranking", return_value=["a"]), \
         patch.object(searcher, "_sparse_ranking", return_value=[]):
        results = searcher.search("query", top_k=1)

    assert results[0].citation == "Doc > Section a"
