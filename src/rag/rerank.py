"""Cross-encoder reranking of the hybrid-fused candidates.

Dense/sparse fusion is good at finding a broad, plausibly-relevant set
quickly, but a bi-encoder's similarity score is a fairly coarse signal - it
compares a query embedding to a chunk embedding independently. A
cross-encoder instead reads the query and chunk together, attending across
both at once, and gives a much sharper relevance signal for the final top-N
cut that actually gets shown to the answer-generation step.
"""

from sentence_transformers import CrossEncoder

from hybrid_search import HybridSearcher, SearchResult

RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class Reranker:
    def __init__(self) -> None:
        self._model = CrossEncoder(RERANKER_MODEL)

    def rerank(self, query: str, candidates: list[SearchResult], top_n: int = 3) -> list[SearchResult]:
        pairs = [[query, c.text] for c in candidates]
        scores = self._model.predict(pairs)
        reranked = sorted(zip(candidates, scores), key=lambda cs: cs[1], reverse=True)
        return [c for c, _ in reranked[:top_n]]


if __name__ == "__main__":
    query = "what happens if my flight is delayed for a technical issue"
    searcher = HybridSearcher()
    candidates = searcher.search(query, top_k=8)

    reranker = Reranker()
    top = reranker.rerank(query, candidates, top_n=3)

    print(f"Query: {query}\n")
    print("Hybrid-fused candidates (pre-rerank):")
    for c in candidates:
        print(f"  {c.fused_score:.4f}  {c.citation}")

    print("\nAfter cross-encoder reranking:")
    for c in top:
        print(f"  {c.citation}")
