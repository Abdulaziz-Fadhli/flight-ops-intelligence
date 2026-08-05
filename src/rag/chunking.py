"""Structure-aware chunking for the airline policy documents.

Splits each markdown doc on its `##` section headers rather than a fixed
token window - a policy section is the natural retrieval unit here, and
splitting mid-section would separate a rule from the condition that
triggers it (e.g. splitting a compensation table from the delay-reason
paragraph that explains it).
"""

import glob
import os
import re
from dataclasses import dataclass

POLICY_DOCS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "policy_docs")


@dataclass
class Chunk:
    chunk_id: str
    doc_title: str
    section_title: str
    text: str

    @property
    def citation(self) -> str:
        return f"{self.doc_title} > {self.section_title}"


def _doc_title(markdown_text: str, fallback: str) -> str:
    match = re.match(r"^#\s+(.+)$", markdown_text.strip().splitlines()[0])
    return match.group(1).strip() if match else fallback


def chunk_document(path: str) -> list[Chunk]:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    doc_title = _doc_title(text, os.path.basename(path))
    doc_slug = os.path.splitext(os.path.basename(path))[0]

    # Split on level-2 headers, keeping the header text with its section.
    sections = re.split(r"\n(?=## )", text)
    chunks: list[Chunk] = []
    for i, section in enumerate(sections):
        section = section.strip()
        if not section or section.startswith("# "):
            # the top-level "# Title" line alone, with no body - skip it
            if section.startswith("# ") and "\n" not in section:
                continue
        header_match = re.match(r"^##\s+(.+)$", section.splitlines()[0]) if section.startswith("##") else None
        section_title = header_match.group(1).strip() if header_match else "Overview"

        if not section:
            continue

        chunks.append(
            Chunk(
                chunk_id=f"{doc_slug}::{i}",
                doc_title=doc_title,
                section_title=section_title,
                text=section,
            )
        )
    return chunks


def chunk_all_documents() -> list[Chunk]:
    all_chunks: list[Chunk] = []
    for path in sorted(glob.glob(os.path.join(POLICY_DOCS_DIR, "*.md"))):
        all_chunks.extend(chunk_document(path))
    return all_chunks


if __name__ == "__main__":
    chunks = chunk_all_documents()
    print(f"Chunked {len(chunks)} sections from policy docs:")
    for c in chunks:
        print(f"  [{c.chunk_id}] {c.citation} ({len(c.text)} chars)")
