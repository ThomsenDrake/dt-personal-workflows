"""Pure casting / chunking / prompt logic for the audiobook pipeline.

No SDK imports: unit-testable without mistralai-workflows. LLM and TTS calls live
in epub_audiobook.py and consume the prompt builders here.
"""
from __future__ import annotations

import json
import re

NARRATOR = "Narrator"

# English preset voices shipped with Voxtral 3 (see docs.mistral.ai/studio/audio/
# text-to-speech/voices). Voxtral 3 also still accepts the Voxtral 2 slugs
# (gb_jane_neutral etc.); the catalog below uses the Voxtral 3 IDs.
VOICE_CATALOG: list[dict] = [
    {"voice_id": "voice_478", "name": "Sheila", "language": "en-US", "gender": "female"},
    {"voice_id": "voice_447", "name": "Margaret", "language": "en-US", "gender": "female"},
    {"voice_id": "voice_485", "name": "Rob", "language": "en-US", "gender": "male"},
    {"voice_id": "voice_3150", "name": "James", "language": "en-US", "gender": "male"},
    {"voice_id": "voice_1787", "name": "Mary", "language": "en-GB", "gender": "female"},
    {"voice_id": "voice_1786", "name": "Sky", "language": "en-GB", "gender": "female"},
    {"voice_id": "voice_1788", "name": "David", "language": "en-GB", "gender": "male"},
    {"voice_id": "voice_590", "name": "Harriet", "language": "en-GB", "gender": "female"},
    {"voice_id": "voice_591", "name": "Fred", "language": "en-GB", "gender": "male"},
    {"voice_id": "voice_3149", "name": "Hugo", "language": "en-GB", "gender": "male"},
    {"voice_id": "voice_3147", "name": "Howard", "language": "en-GB", "gender": "male"},
]

DEFAULT_NARRATOR_VOICE = "voice_478"  # Sheila, en-US

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?…])\s+|\n+")


def chunk_text(text: str, chunk_chars: int) -> list[str]:
    """Split text into chunks of at most chunk_chars, preferring sentence boundaries."""
    if chunk_chars <= 0:
        raise ValueError("chunk_chars must be positive")
    text = text.strip()
    if len(text) <= chunk_chars:
        return [text] if text else []
    chunks: list[str] = []
    current = ""
    for piece in _SENTENCE_SPLIT.split(text):
        if not piece:
            continue
        while len(piece) > chunk_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(piece[:chunk_chars])
            piece = piece[chunk_chars:]
        if not current:
            current = piece
        elif len(current) + 1 + len(piece) <= chunk_chars:
            current = current + " " + piece
        else:
            chunks.append(current)
            current = piece
    if current:
        chunks.append(current)
    return chunks


def batch_chapters(chapters: list[dict], batch_chars: int) -> list[dict]:
    """Group chapters into scan batches of about batch_chars each.

    Returns [{indices: [..], text: "..."}]; every chapter appears in exactly one batch,
    so the character scan always covers the entire book.
    """
    batches: list[dict] = []
    indices: list[int] = []
    text = ""
    for i, ch in enumerate(chapters):
        piece = ch["text"]
        if not piece:
            continue
        if text and len(text) + len(piece) + 2 > batch_chars:
            batches.append({"indices": indices, "text": text})
            indices, text = [], ""
        indices.append(i)
        text = (text + "\n\n" + piece) if text else piece
    if indices:
        batches.append({"indices": indices, "text": text})
    return batches


def assign_voices(
    characters: list[dict],
    narrator_voice: str = DEFAULT_NARRATOR_VOICE,
    voice_overrides: dict | None = None,
    max_characters: int = 8,
    catalog: list[dict] = VOICE_CATALOG,
) -> tuple[list[dict], list[str]]:
    """Assign a distinct preset voice to each of the top characters; narrator gets narrator_voice.

    characters: [{name, description, gender, mentions}] from the full-book scan + merge.
    Returns (cast, warnings) where cast is a list of
    {name, description, gender, mentions, voice_id, voice_name, is_narrator}.
    """
    warnings: list[str] = []
    overrides = {k.strip().lower(): v for k, v in (voice_overrides or {}).items()}
    remaining_overrides = dict(voice_overrides or {})
    catalog_by_id = {v["voice_id"]: v for v in catalog}

    narrator_entry = {
        "name": NARRATOR,
        "description": "Narration and any character without a dedicated voice",
        "gender": "",
        "mentions": 0,
        "voice_id": narrator_voice,
        "voice_name": catalog_by_id.get(narrator_voice, {}).get("name", narrator_voice),
        "is_narrator": True,
    }
    if narrator_voice not in catalog_by_id:
        warnings.append(f"narrator_voice {narrator_voice!r} is not in the built-in catalog; passed through as-is")

    voiced = sorted(
        (c for c in characters if c.get("name")),
        key=lambda c: (-(c.get("mentions") or 0), c["name"].lower()),
    )[: max(0, max_characters)]
    overflow = sorted(
        (c for c in characters if c.get("name")),
        key=lambda c: (-(c.get("mentions") or 0), c["name"].lower()),
    )[max(0, max_characters):]
    if overflow:
        names = ", ".join(c["name"] for c in overflow[:10])
        more = f" (+{len(overflow) - 10} more)" if len(overflow) > 10 else ""
        warnings.append(
            f"{len(overflow)} character(s) beyond the {max_characters}-voice cap fold into the narrator: {names}{more}"
        )

    used: set[str] = {narrator_voice}
    cast: list[dict] = [narrator_entry]
    for ch in voiced:
        name = ch["name"]
        override = overrides.pop(name.lower(), None)
        if name in remaining_overrides:
            remaining_overrides.pop(name)
        else:
            remaining_overrides.pop(name.lower(), None)
        if override:
            if override not in catalog_by_id:
                warnings.append(f"voice override {override!r} for {name!r} is not in the built-in catalog; passed through as-is")
            voice_id = override
        else:
            gender = (ch.get("gender") or "").lower()
            candidates = [v for v in catalog if v["voice_id"] not in used]
            preferred = [v for v in candidates if gender and v["gender"] == gender]
            pool = preferred or candidates
            if not pool:
                warnings.append(f"no free preset voice left for {name!r}; narrator voice used")
                voice_id = narrator_voice
            else:
                voice_id = pool[0]["voice_id"]
        used.add(voice_id)
        cast.append({
            "name": name,
            "description": ch.get("description", ""),
            "gender": ch.get("gender", ""),
            "mentions": ch.get("mentions", 0),
            "voice_id": voice_id,
            "voice_name": catalog_by_id.get(voice_id, {}).get("name", voice_id),
            "is_narrator": False,
        })
    for leftover in remaining_overrides:
        warnings.append(f"voice override for unknown character {leftover!r} was ignored")
    return cast, warnings


def cast_voice_map(cast: list[dict]) -> dict[str, str]:
    return {c["name"].lower(): c["voice_id"] for c in cast}


def character_scan_prompt(batch_text: str) -> str:
    return (
        "You are preparing an audiobook cast for a novel. Below is a batch of the book's text "
        "(one or more chapters). Extract every CHARACTER that appears in this batch: named "
        "people (and distinct named non-human speakers, if any).\n\n"
        "Return ONLY a JSON object: {\"characters\": [{\"name\": str, \"aliases\": [str], "
        "\"description\": str (1 line: role/appearance), \"gender\": \"female\"|\"male\"|\"\", "
        "\"mentions\": int (approx. count of name/alias occurrences in this batch)}}. "
        "Do not invent characters. Do not include the narrator.\n\n"
        "BOOK BATCH:\n" + batch_text
    )


def cast_merge_prompt(batch_casts_json: str) -> str:
    return (
        "You are merging per-batch character lists from ONE book into a single canonical cast. "
        "Reconcile aliases (e.g. \"Tom\" and \"Thomas\" are one character): pick the most common "
        "full name as \"name\", list the others under \"aliases\", sum the mentions, keep the best "
        "description, and keep gender if any batch gave one. Drop duplicates. Do not invent "
        "characters that appear in no batch.\n\n"
        "Return ONLY a JSON object: {\"characters\": [{\"name\": str, \"aliases\": [str], "
        "\"description\": str, \"gender\": \"female\"|\"male\"|\"\", \"mentions\": int}]}.\n\n"
        "BATCH CASTS:\n" + batch_casts_json
    )


def segment_prompt(chapter_title: str, chapter_text: str, cast_json: str) -> str:
    return (
        "You are scripting an audiobook chapter for text-to-speech. Split the chapter into "
        "ordered segments, each spoken by exactly one speaker. Use ONLY these speakers "
        "(exact names): " + ", ".join(c["name"] for c in json.loads(cast_json)) + ". "
        "Attribute dialogue to the character who speaks it (use the cast descriptions to "
        "decide); everything else, including dialogue tags and description, goes to "
        + NARRATOR + ". Keep each segment's text verbatim from the chapter (you may strip "
        "stage-direction asterisks); do not paraphrase, summarize, or add text. Merge "
        "adjacent segments by the same speaker.\n\n"
        "Return ONLY a JSON object: {\"segments\": [{\"speaker\": str, \"text\": str}]}, "
        "where the concatenation of all segment texts equals the chapter text (modulo "
        "whitespace).\n\n"
        "CAST:\n" + cast_json + "\n\n"
        "CHAPTER TITLE: " + chapter_title + "\n\n"
        "CHAPTER TEXT:\n" + chapter_text
    )


_JSON_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


def parse_json_object(raw: str) -> dict:
    """Parse a JSON object from an LLM response, tolerating markdown fences and prose."""
    if not raw:
        raise ValueError("empty LLM response")
    text = raw.strip()
    fence = _JSON_FENCE.search(text)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise ValueError(f"LLM response is not a JSON object: {raw[:200]!r}")


def render_script_markdown(book_title: str, cast: list[dict], chapters: list[dict]) -> str:
    """chapters: [{title, segments: [{speaker, text}]}]. Renders the human-readable cast script."""
    lines = [f"# Cast script: {book_title}", "", "## Cast", ""]
    for c in cast:
        role = "narrator" if c["is_narrator"] else (c.get("description") or "character")
        lines.append(f"- **{c['name']}** — voice `{c['voice_id']}` ({c['voice_name']}) — {role}")
    lines.append("")
    for i, ch in enumerate(chapters, 1):
        lines.append(f"## Chapter {i}: {ch['title']}")
        lines.append("")
        for seg in ch.get("segments", []):
            lines.append(f"**{seg['speaker']}:** {seg['text']}")
            lines.append("")
    return "\n".join(lines).strip() + "\n"
