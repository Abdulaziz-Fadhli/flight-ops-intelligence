"""Grounded answer generation using a small local model - no external API
key or per-call cost, so the whole RAG pipeline stays free to re-run for
grading.

The model is instructed to answer using only the retrieved chunks; the
citation list is attached programmatically afterward rather than trusting
the model to reproduce citations verbatim, since small local models are
unreliable at faithfully echoing structured metadata.
"""

from transformers import pipeline

from hybrid_search import SearchResult

GENERATION_MODEL = "google/flan-t5-base"

_generator = None


def _get_generator():
    global _generator
    if _generator is None:
        _generator = pipeline("text2text-generation", model=GENERATION_MODEL, max_new_tokens=200)
    return _generator


def generate_answer(query: str, context_chunks: list[SearchResult]) -> dict:
    context_block = "\n\n".join(f"[{i+1}] {c.text}" for i, c in enumerate(context_chunks))
    prompt = (
        "You are an airline operations assistant. Using only the policy excerpts "
        "below, write a complete, self-contained sentence answering the question. "
        "Do not just repeat a heading or a single word - explain the relevant rule. "
        "If the excerpts don't fully cover the question, say what they do cover.\n\n"
        f"Policy excerpts:\n{context_block}\n\n"
        f"Question: {query}\n"
        "Answer in one or two full sentences:"
    )

    generator = _get_generator()
    result = generator(prompt)[0]["generated_text"].strip()

    return {
        "query": query,
        "answer": result,
        "citations": [
            {"citation": c.citation, "excerpt": c.text[:200] + ("..." if len(c.text) > 200 else "")}
            for c in context_chunks
        ],
    }
