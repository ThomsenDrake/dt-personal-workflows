"""
epub-voxtral-audiobook — Mistral Workflow (Studio, Temporal-backed, OBO)

Turns a .epub from the caller's Google Drive into a multi-character casted
audiobook synthesized with Voxtral (POST /v1/audio/speech), and uploads the
result back to Google Drive.

Pipeline (every side effect is an @workflows.activity, so steps are durable and
retried; the entrypoint stays deterministic):

  1. check_epub            Drive metadata: fail early on non-epub / oversize files
  2. fetch_and_parse_epub  download via the google_drive_mcp connector slot, parse
                           the epub (zip + OPF spine) into chapter texts
  3. ensure_output_folder  create/reuse the Drive output folder
  4. scan_characters       LLM scans the ENTIRE book in batches (~batch_chars each),
                           then an LLM merge pass dedupes aliases into one cast
  5. segment_chapter       per chapter, LLM splits text into speaker-labeled segments
                           using the global cast (deterministic voice assignment runs
                           in the entrypoint via cast_logic.assign_voices)
  6. synthesize_chapter    Voxtral TTS per sentence-bounded chunk (bounded concurrency),
                           ffmpeg concat per chapter, split oversize files into parts,
                           upload parts to Drive
  7. build_merged          ffmpeg concat of all chapters into one file (skipped when
                           above max_merged_bytes), uploaded to Drive
  8. publish_outputs       upload the cast script (Markdown) and manifest (JSON)

Identity: on_behalf_of=True — started from Vibe/Studio by a person, so the
google_drive_mcp slot resolves THAT user's Drive credential (no secrets in code;
if missing, the run pauses with an OAuth auth URL). Mistral API calls (chat for
scripting, audio.speech for TTS) use the worker's service account via
get_mistral_client(), like the other workers in this monorepo.

Deploy: workflows/epub-voxtral-audiobook/worker in the dt-personal-workflows
monorepo, image deploy/docker/Dockerfile.epub-voxtral-audiobook (adds ffmpeg),
managed deployment "epub-audiobook-worker" (hardened=true — OBO requires it):

  execute_workflow(
      workflow_identifier="epub-voxtral-audiobook",
      deployment_name="epub-audiobook-worker",
      input={"epub_file_id": "<drive file id>"},
  )
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.client import get_mistral_client
from mistralai.workflows.plugins.mistralai.connectors import ConnectorClient, connector, uses_connectors

from worker.workflows import cast_logic, epub_parse
from worker.workflows.cast_logic import NARRATOR
from worker.workflows.models import (
    CastMember,
    ChapterResult,
    DriveFile,
    EpubAudiobookInput,
    EpubAudiobookOutput,
)

logger = logging.getLogger(__name__)

gdrive = connector("google_drive_mcp")

_FOLDER_MIME = "application/vnd.google-apps.folder"
_SAFE_TITLE = re.compile(r"[^\w\-.() ]+")


def _safe_name(name: str) -> str:
    cleaned = _SAFE_TITLE.sub("_", name).strip().strip(".")
    return cleaned[:120] or "audiobook"


# ── Connector tool plumbing ───────────────────────────────────────────────────


def _as_dict(res) -> dict:
    if res is None:
        return {}
    if isinstance(res, dict):
        content = res.get("content")
        if isinstance(content, list):
            text = "\n".join(
                b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
            )
            try:
                return json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return {"_text": text}
        return res
    if hasattr(res, "model_dump"):
        return res.model_dump()
    if hasattr(res, "__dict__"):
        return {k: v for k, v in vars(res).items() if not k.startswith("_")}
    return {"_raw": str(res)}


def _b64_payload(d: dict) -> str:
    content = d.get("content")
    if isinstance(content, str) and content:
        return content
    structured = d.get("structuredContent")
    if isinstance(structured, dict) and isinstance(structured.get("content"), str):
        return structured["content"]
    text = d.get("_text", "")
    raise ValueError(f"download_file_content returned no base64 content (keys: {sorted(d.keys())}; text head: {text[:120]!r})")


async def _drive_metadata(gdrive: ConnectorClient, file_id: str) -> dict:
    return _as_dict(await gdrive.call_tool("get_file_metadata", {"file_id": file_id}))


async def _drive_download(gdrive: ConnectorClient, file_id: str) -> bytes:
    res = _as_dict(await gdrive.call_tool("download_file_content", {"file_id": file_id}))
    return base64.b64decode(_b64_payload(res))


async def _drive_search(gdrive: ConnectorClient, query: str, page_size: int = 20) -> list[dict]:
    res = _as_dict(await gdrive.call_tool("search_files", {"query": query, "page_size": page_size}))
    return res.get("files") or []


def _q(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


async def _drive_create_folder(gdrive: ConnectorClient, title: str, parent_id: str | None) -> dict:
    args: dict = {"title": title, "content_mime_type": _FOLDER_MIME}
    if parent_id:
        args["parent_id"] = parent_id
    return _as_dict(await gdrive.call_tool("create_file", args))


async def _drive_upload_bytes(
    gdrive: ConnectorClient,
    title: str,
    folder_id: str,
    data: bytes,
    mime_type: str,
) -> dict:
    """Create or update (idempotent by title within the folder) a binary file."""
    query = f"title = {_q(title)} and parent_id = {_q(folder_id)}"
    existing = await _drive_search(gdrive, query)
    b64 = base64.b64encode(data).decode()
    if existing:
        res = _as_dict(await gdrive.call_tool(
            "update_file",
            {"file_id": existing[0]["id"], "base64_content": b64, "content_mime_type": mime_type},
        ))
    else:
        res = _as_dict(await gdrive.call_tool(
            "create_file",
            {
                "title": title,
                "parent_id": folder_id,
                "base64_content": b64,
                "content_mime_type": mime_type,
                "disable_conversion_to_google_type": True,
            },
        ))
    return res


def _drive_file(res: dict, name: str) -> DriveFile:
    size = res.get("file_size")
    return DriveFile(
        name=name,
        id=res.get("id") or "",
        view_url=res.get("view_url"),
        mime_type=res.get("mime_type"),
        size_bytes=int(size) if size not in (None, "") else None,
    )


# ── Mistral API helpers (worker service account) ─────────────────────────────


async def _chat_json(model: str, prompt: str) -> dict:
    client = get_mistral_client()
    resp = await client.chat.complete_async(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return cast_logic.parse_json_object(content)


async def _tts_chunk(model: str, voice_id: str, text: str, response_format: str) -> bytes:
    client = get_mistral_client()
    resp = await client.audio.speech.complete_async(
        model=model,
        input=text,
        voice_id=voice_id,
        response_format=response_format,
    )
    audio_data = getattr(resp, "audio_data", None)
    if not audio_data:
        raise ValueError("audio/speech response carried no audio_data")
    return base64.b64decode(audio_data)


# ── ffmpeg helpers ────────────────────────────────────────────────────────────


def _run(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed: {proc.stderr.strip()[:500]}")


def _concat(inputs: list[Path], out: Path, workdir: Path) -> None:
    list_file = workdir / f"concat-{out.stem}.txt"
    list_file.write_text("".join(f"file '{p.resolve()}'\n" for p in inputs))
    _run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
          "-i", str(list_file), "-c", "copy", str(out)])


def _probe_duration(path: Path) -> float | None:
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)],
            capture_output=True, text=True,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return float(proc.stdout.strip())
    except Exception:
        pass
    return None


def _split_if_needed(path: Path, max_part_bytes: int, workdir: Path) -> list[Path]:
    if path.stat().st_size <= max_part_bytes:
        return [path]
    out_pattern = workdir / f"{path.stem}-part%03d{path.suffix}"
    _run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(path),
          "-f", "segment", "-segment_time", "600", "-c", "copy", str(out_pattern)])
    parts = sorted(workdir.glob(f"{path.stem}-part*{path.suffix}"))
    return parts or [path]


# ── Activities ────────────────────────────────────────────────────────────────


@workflows.activity(start_to_close_timeout=timedelta(minutes=5), retry_policy_max_attempts=2)
async def check_epub(file_id: str, max_epub_bytes: int, gdrive: ConnectorClient = Depends(gdrive)) -> dict:
    meta = await _drive_metadata(gdrive, file_id)
    if not meta.get("id"):
        raise ValueError(f"Drive file {file_id!r} not found or not readable")
    size = meta.get("file_size")
    size_int = int(size) if size not in (None, "") else 0
    mime = (meta.get("mime_type") or "").lower()
    title = meta.get("title") or ""
    if size_int > max_epub_bytes:
        raise ValueError(
            f"epub {title!r} is {size_int} bytes, above max_epub_bytes={max_epub_bytes}; "
            "raise the input limit or use a smaller file"
        )
    if "epub" not in mime and not title.lower().endswith(".epub"):
        raise ValueError(f"Drive file {title!r} does not look like an epub (mime {mime!r})")
    return {"title": title, "mime_type": mime, "size_bytes": size_int}


@workflows.activity(start_to_close_timeout=timedelta(minutes=15), retry_policy_max_attempts=2)
async def fetch_and_parse_epub(
    file_id: str,
    max_book_chars: int,
    gdrive: ConnectorClient = Depends(gdrive),
) -> dict:
    data = await _drive_download(gdrive, file_id)
    book = epub_parse.parse_epub(data, max_book_chars=max_book_chars)
    truncated = [c["title"] for c in book["chapters"] if c.get("truncated")]
    return {
        "title": book["title"],
        "author": book["author"],
        "chapters": book["chapters"],
        "total_chars": book["total_chars"],
        "truncated_chapters": truncated,
    }


@workflows.activity(start_to_close_timeout=timedelta(minutes=10), retry_policy_max_attempts=2)
async def ensure_output_folder(
    book_title: str,
    output_folder_id: str | None,
    gdrive: ConnectorClient = Depends(gdrive),
) -> str:
    if output_folder_id:
        return output_folder_id
    root_name = "epub-audiobook-output"
    existing = await _drive_search(gdrive, f"title = {_q(root_name)} and mimeType = {_q(_FOLDER_MIME)}")
    root_id = existing[0]["id"] if existing else (await _drive_create_folder(gdrive, root_name, None))["id"]
    book_folder = _safe_name(book_title)
    existing = await _drive_search(
        gdrive, f"title = {_q(book_folder)} and parent_id = {_q(root_id)} and mimeType = {_q(_FOLDER_MIME)}"
    )
    if existing:
        return existing[0]["id"]
    return (await _drive_create_folder(gdrive, book_folder, root_id))["id"]


@workflows.activity(start_to_close_timeout=timedelta(hours=4), retry_policy_max_attempts=1)
async def scan_characters(chapters: list[dict], batch_chars: int, model: str) -> dict:
    """Scan the ENTIRE book in batches, then merge the per-batch casts into one."""
    batches = cast_logic.batch_chapters(chapters, batch_chars)
    batch_casts: list[dict] = []
    for i, batch in enumerate(batches):
        logger.info("character scan batch %d/%d (%d chars)", i + 1, len(batches), len(batch["text"]))
        parsed = await _chat_json(model, cast_logic.character_scan_prompt(batch["text"]))
        chars = parsed.get("characters") or []
        batch_casts.append({"batch": i, "indices": batch["indices"], "characters": chars})
    merged = await _chat_json(
        model, cast_logic.cast_merge_prompt(json.dumps(batch_casts, ensure_ascii=False))
    )
    characters = merged.get("characters") or []
    characters.sort(key=lambda c: (-(c.get("mentions") or 0), (c.get("name") or "").lower()))
    return {"characters": characters, "batches": len(batches)}


@workflows.activity(start_to_close_timeout=timedelta(minutes=30), retry_policy_max_attempts=2)
async def segment_chapter(index: int, title: str, text: str, cast_json: str, model: str) -> dict:
    if not text.strip():
        return {"index": index, "title": title, "segments": [], "warnings": ["empty chapter skipped"]}
    parsed = await _chat_json(model, cast_logic.segment_prompt(title, text, cast_json))
    valid = {c["name"].lower() for c in json.loads(cast_json)}
    segments: list[dict] = []
    warnings: list[str] = []
    for seg in parsed.get("segments") or []:
        speaker = (seg.get("speaker") or "").strip()
        seg_text = (seg.get("text") or "").strip()
        if not seg_text:
            continue
        if speaker.lower() not in valid:
            warnings.append(f"unknown speaker {speaker!r} mapped to narrator")
            speaker = NARRATOR
        if segments and segments[-1]["speaker"] == speaker:
            segments[-1]["text"] += " " + seg_text
        else:
            segments.append({"speaker": speaker, "text": seg_text})
    joined = " ".join(s["text"] for s in segments)
    if text and len(joined) < 0.5 * len(text):
        warnings.append("segmented text covers <50% of the chapter; the LLM may have dropped content")
    return {"index": index, "title": title, "segments": segments, "warnings": warnings}


@workflows.activity(start_to_close_timeout=timedelta(hours=3), retry_policy_max_attempts=2)
async def synthesize_chapter(
    index: int,
    title: str,
    segments: list[dict],
    voice_map: dict,
    narrator_voice: str,
    tts_model: str,
    response_format: str,
    chunk_chars: int,
    tts_concurrency: int,
    max_part_bytes: int,
    run_dir: str,
    folder_id: str,
    gdrive: ConnectorClient = Depends(gdrive),
) -> dict:
    workdir = Path(run_dir)
    workdir.mkdir(parents=True, exist_ok=True)
    ext = "mp3" if response_format == "mp3" else response_format
    jobs: list[tuple[int, str, str]] = []
    for s_i, seg in enumerate(segments):
        voice_id = voice_map.get(seg["speaker"].lower(), narrator_voice)
        for c_i, chunk in enumerate(cast_logic.chunk_text(seg["text"], chunk_chars)):
            jobs.append((len(jobs), voice_id, chunk))
    if not jobs:
        return {"index": index, "title": title, "chars": 0, "segments": 0, "files": [], "warnings": ["no segments"]}

    sem = asyncio.Semaphore(tts_concurrency)

    async def one(job: tuple[int, str, str]) -> tuple[int, bytes]:
        j, voice_id, chunk = job
        async with sem:
            audio = await _tts_chunk(tts_model, voice_id, chunk, response_format)
        return j, audio

    results = await asyncio.gather(*(one(j) for j in jobs))
    chunk_paths: list[Path] = []
    for j, audio in sorted(results, key=lambda r: r[0]):
        p = workdir / f"ch{index:04d}-{j:05d}.{ext}"
        p.write_bytes(audio)
        chunk_paths.append(p)

    chapter_path = workdir / f"ch{index:04d}-{_safe_name(title)}.{ext}"
    _concat(chunk_paths, chapter_path, workdir)
    for p in chunk_paths:
        p.unlink(missing_ok=True)
    duration = _probe_duration(chapter_path)
    parts = _split_if_needed(chapter_path, max_part_bytes, workdir)
    mime = {"mp3": "audio/mpeg", "wav": "audio/wav", "flac": "audio/flac", "opus": "audio/ogg", "pcm": "audio/pcm"}.get(
        response_format, "application/octet-stream"
    )
    files: list[DriveFile] = []
    for k, part in enumerate(parts):
        name = f"{_safe_name(title)}-ch{index + 1:03d}" + (f"-part{k + 1:03d}" if len(parts) > 1 else "") + f".{ext}"
        res = await _drive_upload_bytes(gdrive, name, folder_id, part.read_bytes(), mime)
        files.append(_drive_file(res, name))
    chapter_path.unlink(missing_ok=True)
    return {
        "index": index,
        "title": title,
        "chars": sum(len(s["text"]) for s in segments),
        "segments": len(segments),
        "duration_seconds": duration,
        "files": [f.model_dump() for f in files],
        "local_parts": [str(p) for p in parts],
        "warnings": [],
    }


@workflows.activity(start_to_close_timeout=timedelta(hours=1), retry_policy_max_attempts=1)
async def build_merged(
    book_title: str,
    local_parts: list[str],
    max_merged_bytes: int,
    folder_id: str,
    gdrive: ConnectorClient = Depends(gdrive),
) -> dict:
    workdir = Path(tempfile.mkdtemp(prefix="epub-merged-"))
    paths = [Path(p) for p in local_parts if p and os.path.exists(p)]
    if not paths:
        return {"file": None, "warning": "chapter audio not on local disk; merged file skipped"}
    merged = workdir / f"{_safe_name(book_title)}-merged.mp3"
    _concat(paths, merged, workdir)
    size = merged.stat().st_size
    if size > max_merged_bytes:
        return {"file": None, "warning": f"merged file is {size} bytes > max_merged_bytes={max_merged_bytes}; skipped (per-chapter files were uploaded)"}
    res = await _drive_upload_bytes(gdrive, merged.name, folder_id, merged.read_bytes(), "audio/mpeg")
    return {"file": _drive_file(res, merged.name).model_dump(), "warning": None}


@workflows.activity(start_to_close_timeout=timedelta(minutes=10), retry_policy_max_attempts=2)
async def publish_text_file(
    name: str,
    content: str,
    mime_type: str,
    folder_id: str,
    gdrive: ConnectorClient = Depends(gdrive),
) -> dict:
    query = f"title = {_q(name)} and parent_id = {_q(folder_id)}"
    existing = await _drive_search(gdrive, query)
    if existing:
        res = _as_dict(await gdrive.call_tool(
            "update_file",
            {"file_id": existing[0]["id"], "text_content": content, "content_mime_type": mime_type},
        ))
    else:
        res = _as_dict(await gdrive.call_tool(
            "create_file",
            {
                "title": name,
                "parent_id": folder_id,
                "text_content": content,
                "content_mime_type": mime_type,
                "disable_conversion_to_google_type": True,
            },
        ))
    return _drive_file(res, name).model_dump()


# ── Workflow ──────────────────────────────────────────────────────────────────


@workflows.workflow.define(
    name="epub-voxtral-audiobook",
    on_behalf_of=True,
)
@uses_connectors(gdrive)
class EpubVoxtralAudiobook:
    def __init__(self):
        self.steps = {k: "pending" for k in ("fetch", "cast", "segment", "synthesize", "merge", "publish")}

    @workflows.workflow.query(name="get_steps")
    def get_steps(self) -> dict:
        return self.steps

    @workflows.workflow.entrypoint
    async def run(self, payload: EpubAudiobookInput) -> EpubAudiobookOutput:
        import uuid

        p = payload
        run_dir = os.path.join(tempfile.gettempdir(), f"epub-audiobook-{uuid.uuid4().hex[:12]}")
        warnings: list[str] = []

        self.steps["fetch"] = "running"
        await check_epub(p.epub_file_id, p.max_epub_bytes)
        book = await fetch_and_parse_epub(p.epub_file_id, p.max_book_chars)
        if book["truncated_chapters"]:
            warnings.append(
                f"book exceeded max_book_chars={p.max_book_chars}; truncated at chapter(s): "
                + ", ".join(book["truncated_chapters"])
            )
        chapters = book["chapters"]
        folder_id = await ensure_output_folder(book["title"], p.output_folder_id)
        self.steps["fetch"] = "done"

        self.steps["cast"] = "running"
        scan = await scan_characters(chapters, p.batch_chars, p.script_model)
        cast, cast_warnings = cast_logic.assign_voices(
            scan["characters"],
            narrator_voice=p.narrator_voice,
            voice_overrides=p.voice_overrides,
            max_characters=p.max_characters,
        )
        warnings.extend(cast_warnings)
        cast_json = json.dumps(cast, ensure_ascii=False)
        voice_map = cast_logic.cast_voice_map(cast)
        self.steps["cast"] = "done"

        self.steps["segment"] = "running"
        seg_results = await asyncio.gather(*[
            segment_chapter(i, ch["title"], ch["text"], cast_json, p.script_model)
            for i, ch in enumerate(chapters)
        ])
        for r in seg_results:
            warnings.extend(f"chapter {r['index'] + 1} ({r['title']}): {w}" for w in r["warnings"])
        self.steps["segment"] = "done"

        self.steps["synthesize"] = "running"
        chapter_results = await asyncio.gather(*[
            synthesize_chapter(
                r["index"], r["title"], r["segments"], voice_map, p.narrator_voice,
                p.tts_model, p.response_format, p.chunk_chars, p.tts_concurrency,
                p.max_part_bytes, run_dir, folder_id,
            )
            for r in seg_results
        ])
        self.steps["synthesize"] = "done"

        self.steps["merge"] = "running"
        local_parts = [part for cr in chapter_results for part in cr["local_parts"]]
        merged = await build_merged(book["title"], local_parts, p.max_merged_bytes, folder_id)
        if merged.get("warning"):
            warnings.append(merged["warning"])
        self.steps["merge"] = "done"

        self.steps["publish"] = "running"
        script_chapters = [
            {"title": r["title"], "segments": r["segments"]} for r in seg_results
        ]
        script_md = cast_logic.render_script_markdown(book["title"], cast, script_chapters)
        book_slug = _safe_name(book["title"])
        script_file = await publish_text_file(f"{book_slug}-script.md", script_md, "text/markdown", folder_id)
        manifest = {
            "book_title": book["title"],
            "author": book["author"],
            "epub_file_id": p.epub_file_id,
            "output_folder_id": folder_id,
            "cast": cast,
            "tts_model": p.tts_model,
            "script_model": p.script_model,
            "response_format": p.response_format,
            "chapters": [
                {k: v for k, v in cr.items() if k != "local_parts"} for cr in chapter_results
            ],
            "merged_file": merged.get("file"),
            "stats": {
                "chapters": len(chapters),
                "segments": sum(cr["segments"] for cr in chapter_results),
                "book_chars": book["total_chars"],
                "scan_batches": scan["batches"],
                "tts_chars": sum(cr["chars"] for cr in chapter_results),
            },
            "warnings": warnings,
        }
        manifest_file = await publish_text_file(
            f"{book_slug}-manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False),
            "application/json", folder_id,
        )
        self.steps["publish"] = "done"

        all_files = [DriveFile(**f) for cr in chapter_results for f in cr["files"]]
        if merged.get("file"):
            all_files.append(DriveFile(**merged["file"]))
        all_files.append(DriveFile(**script_file))
        all_files.append(DriveFile(**manifest_file))
        return EpubAudiobookOutput(
            book_title=book["title"],
            epub_file_id=p.epub_file_id,
            output_folder_id=folder_id,
            cast=[CastMember(**c) for c in cast],
            chapters=[ChapterResult(**{k: v for k, v in cr.items() if k != "local_parts"}) for cr in chapter_results],
            files=all_files,
            merged_file=DriveFile(**merged["file"]) if merged.get("file") else None,
            script_file=DriveFile(**script_file),
            manifest_file=DriveFile(**manifest_file),
            stats=manifest["stats"],
            warnings=warnings,
        )
