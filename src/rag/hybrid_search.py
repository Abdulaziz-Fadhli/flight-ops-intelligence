"""Hybrid retrieval: dense (embeddings via Chroma) + sparse (BM25 keyword
search), fused with Reciprocal Rank Fusion.

Dense search alone misses exact terms an embedding model was never trained
to weight heavily - a delay reason code like "ATC" or "TECHNICAL" is
exactly this kind of token. BM25 catches those; RRF combines both rankings
without needing to normalize incomparable similarity scores against BM25
scores - it only needs each method's *rank order*.
"""

import os
import pickle
from dataclasses import dataclass

import chromadb
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer

from build_index import BM25_PATH, CHROMA_PATH, COLLECTION_NAME, EMBEDDING_MODEL, _tokenize


@dataclass
class SearchResult:
    chunk_id: str
    text: str
    doc_title: str
    section_title: str
    fused_score: float

    @property
    def citation(self) -> str:
        return f"{self.doc_title} > {self.section_title}"


class HybridSearcher:
    def __init__(self) -> None:
        self._model = SentenceTransformer(EMBEDDING_MODEL)
        self._client = chromadb.PersistentClient(path=CHROMA_PATH)
        self._collection = self._client.get_collection(COLLECTION_NAME)

        with open(BM25_PATH, "rb") as f:
            corpus = pickle.load(f)
        self._bm25_chunk_ids = corpus["chunk_ids"]
        self._bm25_texts = corpus["texts"]
        self._bm25_metadatas = corpus["metadatas"]
        self._bm25 = BM25Okapi(corpus["tokenized_texts"])

    def _dense_ranking(self, query: str, top_k: int) -> list[str]:
        query_embedding = self._model.encode([query]).tolist()
        results = self._collection.query(query_embeddings=query_embedding, n_results=top_k)
        return results["ids"][0]

    def _sparse_ranking(self, query: str, top_k: int) -> list[str]:
        scores = self._bm25.get_scores(_tokenize(query))
        ranked_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        return [self._bm25_chunk_ids[i] for i in ranked_indices]

    def search(self, query: str, top_k: int = 10, rrf_k: int = 60) -> list[SearchResult]:
        dense_ids = self._dense_ranking(query, top_k)
        sparse_ids = self._sparse_ranking(query, top_k)

        # Reciprocal Rank Fusion: score(doc) = sum over rankings of 1 / (rrf_k + rank)
        fused_scores: dict[str, float] = {}
        for rank, chunk_id in enumerate(dense_ids):
            fused_scores[chunk_id] = fused_scores.get(chunk_id, 0.0) + 1.0 / (rrf_k + rank + 1)
        for rank, chunk_id in enumerate(sparse_ids):
            fused_scores[chunk_id] = fused_scores.get(chunk_id, 0.0) + 1.0 / (rrf_k + rank + 1)

        id_to_text = dict(zip(self._bm25_chunk_ids, self._bm25_texts))
        id_to_meta = dict(zip(self._bm25_chunk_ids, self._bm25_metadatas))

        ranked = sorted(fused_scores.items(), key=lambda kv: kv[1], reverse=True)
        return [
            SearchResult(
                chunk_id=chunk_id,
                text=id_to_text[chunk_id],
                doc_title=id_to_meta[chunk_id]["doc_title"],
                section_title=id_to_meta[chunk_id]["section_title"],
                fused_score=score,
            )
            for chunk_id, score in ranked
        ]


if __name__ == "__main__":
    searcher = HybridSearcher()
    for result in searcher.search("what happens if my flight is delayed for a technical issue", top_k=5):
        print(f"{result.fused_score:.4f}  {result.citation}")
