"""RAGAS-style retrieval evaluation: context precision and average
query-to-chunk similarity (a recall proxy), computed the same way full
RAGAS does it under the hood for these two metrics - cosine similarity
between query and retrieved-chunk embeddings - without needing an LLM
judge or the ragas package itself.

Doesn't replace generation quality checks (answer faithfulness/relevance
genuinely need an LLM judge, which is out of scope for a free, local-only
pipeline) - this measures whether retrieval is actually pulling relevant
context before generation ever runs, which is the retrieval half of RAG
correctness and the half that's fully measurable without an API call.
"""

import numpy as np
from sentence_transformers import SentenceTransformer

from build_index import EMBEDDING_MODEL
from hybrid_search import HybridSearcher, SearchResult
from rerank import Reranker

RELEVANCE_THRESHOLD = 0.30


def evaluate_retrieval(
    query: str,
    retrieved_chunks: list[SearchResult],
    embed_model: SentenceTransformer,
    relevance_threshold: float = RELEVANCE_THRESHOLD,
) -> dict:
    q_emb = embed_model.encode(query, normalize_embeddings=True)
    scores = [
        float(np.dot(q_emb, embed_model.encode(c.text, normalize_embeddings=True)))
        for c in retrieved_chunks
    ]
    relevant = sum(s > relevance_threshold for s in scores)
    return {
        "context_precision": round(relevant / len(scores), 3),
        "avg_similarity": round(sum(scores) / len(scores), 3),
        "chunks_in_context": len(retrieved_chunks),
        "per_chunk_scores": [round(s, 3) for s in scores],
    }


def run() -> None:
    searcher = HybridSearcher()
    reranker = Reranker()
    embed_model = SentenceTransformer(EMBEDDING_MODEL)

    queries = [
        "What compensation am I owed if my flight is delayed 5 hours due to a technical issue?",
        "If my flight is cancelled, what are my options?",
        "How much notice do I get before a gate change right before my flight departs?",
        "Am I entitled to compensation if my flight is delayed because of weather?",
    ]

    print(f"{'='*70}\n  Retrieval evaluation: context precision + avg similarity\n{'='*70}")

    all_reports = []
    for query in queries:
        candidates = searcher.search(query, top_k=8)
        top_chunks = reranker.rerank(query, candidates, top_n=3)
        report = evaluate_retrieval(query, top_chunks, embed_model)
        all_reports.append(report)

        print(f"\nQuery: {query}")
        for chunk, score in zip(top_chunks, report["per_chunk_scores"]):
            flag = "OK" if score > RELEVANCE_THRESHOLD else "LOW"
            print(f"  [{flag}] sim={score:.3f}  {chunk.citation}")
        print(f"  -> context_precision={report['context_precision']}  "
              f"avg_similarity={report['avg_similarity']}")
        if report["context_precision"] < 0.5:
            print("  WARNING: low precision - consider tuning chunk size or rerank top_n")

    avg_precision = round(sum(r["context_precision"] for r in all_reports) / len(all_reports), 3)
    avg_similarity = round(sum(r["avg_similarity"] for r in all_reports) / len(all_reports), 3)
    print(f"\n{'='*70}")
    print(f"  Overall: avg context_precision={avg_precision}, avg similarity={avg_similarity} "
          f"across {len(queries)} queries")
    print(f"{'='*70}")


if __name__ == "__main__":
    run()
