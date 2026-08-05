"""Ties hybrid search -> rerank -> grounded generation into one entrypoint,
optionally enriched with a flight's live current state pulled straight from
the Silver Delta table (via the lightweight `deltalake` reader - no Spark/
JVM needed just to look up one row).
"""

import os
import re
import sys

from deltalake import DeltaTable

from generate import generate_answer
from hybrid_search import HybridSearcher
from rerank import Reranker

SILVER_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "lakehouse", "silver", "flight_state")

_FLIGHT_ID_PATTERN = re.compile(r"\b([A-Z]{2}\d{3,4}-\d{8})\b")


def _lookup_live_status(flight_id: str) -> dict | None:
    if not os.path.isdir(SILVER_PATH):
        return None
    df = DeltaTable(SILVER_PATH).to_pandas()
    match = df[df["flight_id"] == flight_id]
    if match.empty:
        return None
    row = match.iloc[0]
    return {
        "flight_id": row["flight_id"],
        "status": row["status"],
        "gate": row["gate"],
        "delay_minutes": row["delay_minutes"],
        "delay_reason_code": row["delay_reason_code"],
    }


class FlightOpsQA:
    def __init__(self) -> None:
        print("Loading retrieval models (embedding + reranker)...", file=sys.stderr)
        self._searcher = HybridSearcher()
        self._reranker = Reranker()

    def ask(self, query: str, top_k: int = 8, top_n: int = 3) -> dict:
        candidates = self._searcher.search(query, top_k=top_k)
        top_chunks = self._reranker.rerank(query, candidates, top_n=top_n)
        result = generate_answer(query, top_chunks)

        flight_match = _FLIGHT_ID_PATTERN.search(query)
        if flight_match:
            result["live_status"] = _lookup_live_status(flight_match.group(1))

        return result


def _print_result(result: dict) -> None:
    print(f"\nQ: {result['query']}")
    if result.get("live_status"):
        s = result["live_status"]
        print(f"   [live status: {s['flight_id']} is {s['status']}, gate {s['gate']}, "
              f"delay {s['delay_minutes']} min, reason {s['delay_reason_code']}]")
    print(f"A: {result['answer']}")
    print("   Citations:")
    for c in result["citations"]:
        print(f"     - {c['citation']}")


if __name__ == "__main__":
    qa = FlightOpsQA()
    demo_questions = [
        "What compensation am I owed if my flight is delayed 5 hours due to a technical issue?",
        "If my flight is cancelled, what are my options?",
        "How much notice do I get before a gate change right before my flight departs?",
        "Am I entitled to compensation if my flight is delayed because of weather?",
    ]
    for q in demo_questions:
        _print_result(qa.ask(q))
