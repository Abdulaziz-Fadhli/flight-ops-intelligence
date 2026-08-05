"""Embeds every policy-doc chunk and loads them into a persistent Chroma
collection (dense retrieval), and separately builds a BM25 index over the
same chunk texts (keyword retrieval) for the hybrid search stage.
"""

import os
import pickle

import chromadb
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer

from chunking import chunk_all_documents

INDEX_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "data", "rag_index")
CHROMA_PATH = os.path.join(INDEX_ROOT, "chroma")
BM25_PATH = os.path.join(INDEX_ROOT, "bm25.pkl")

EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
COLLECTION_NAME = "flight_ops_policy"


def _tokenize(text: str) -> list[str]:
    return text.lower().split()


def build() -> None:
    os.makedirs(INDEX_ROOT, exist_ok=True)
    chunks = chunk_all_documents()
    print(f"Embedding {len(chunks)} chunks with {EMBEDDING_MODEL}...")

    model = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode([c.text for c in chunks], show_progress_bar=False).tolist()

    client = chromadb.PersistentClient(path=CHROMA_PATH)
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    collection = client.create_collection(COLLECTION_NAME)

    collection.add(
        ids=[c.chunk_id for c in chunks],
        embeddings=embeddings,
        documents=[c.text for c in chunks],
        metadatas=[{"doc_title": c.doc_title, "section_title": c.section_title} for c in chunks],
    )
    print(f"Dense index: {collection.count()} chunks stored in Chroma at {CHROMA_PATH}")

    bm25_corpus = {
        "chunk_ids": [c.chunk_id for c in chunks],
        "tokenized_texts": [_tokenize(c.text) for c in chunks],
        "texts": [c.text for c in chunks],
        "metadatas": [{"doc_title": c.doc_title, "section_title": c.section_title} for c in chunks],
    }
    with open(BM25_PATH, "wb") as f:
        pickle.dump(bm25_corpus, f)
    print(f"Sparse index: BM25 corpus for {len(chunks)} chunks saved to {BM25_PATH}")


if __name__ == "__main__":
    build()
