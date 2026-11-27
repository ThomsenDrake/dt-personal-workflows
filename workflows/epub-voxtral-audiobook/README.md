# EPUB → Voxtral Audiobook (Mistral Studio Workflow)

> This workflow is one self-contained package in the personal Mistral workflows monorepo — see the [repo-root README](../../README.md).

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that turns a `.epub` from the caller's **Google Drive** into a **multi-character casted audiobook** synthesized with **Voxtral** (`POST /v1/audio/speech`), and uploads the result back to Drive. It is `on_behalf_of=True`, so a run started from Vibe/Studio uses the starting user's `google_drive_mcp` credential — no secrets in code.

## Pipeline

~~~mermaid
flowchart LR
    A[check_epub<br/>Drive metadata, size/mime guard] --> B[fetch_and_parse_epub<br/>download + zip/OPF spine -> chapter texts]
    B --> C[ensure_output_folder<br/>epub-audiobook-output/&lt;book&gt;]
    C --> D[scan_characters<br/>LLM scans the ENTIRE book in batches, merge pass dedupes aliases]
    D --> E[segment_chapter x N<br/>per chapter: speaker-labeled segments from the global cast]
    E --> F[synthesize_chapter x N<br/>Voxtral TTS per chunk, ffmpeg concat, split + upload parts]
    F --> G[build_merged<br/>one merged MP3 when under max_merged_bytes]
    G --> H[publish_outputs<br/>script.md + manifest.json]
~~~

All side effects are `@workflows.activity()` functions (durable + retried); the
`@workflows.workflow.define(name="epub-voxtral-audiobook", on_behalf_of=True)`
entrypoint stays deterministic (voice assignment is pure code in
`cast_logic.assign_voices`). The `get_steps` query exposes live per-step progress.

## Casting

- The character scan covers the **whole book**: chapters are grouped into
  `batch_chars` batches (default 20k chars), each batch is scanned by the LLM for
  characters (name/aliases/description/gender/mentions), and one merge pass
  reconciles aliases into a single canonical cast — a character first appearing
  in chapter 40 is still detected and voiced.
- The top `max_characters` (default 8) characters by mentions get **distinct
  preset Voxtral voices**, gender-matched when the LLM tagged a gender; everyone
  else folds into the narrator (`narrator_voice`, default `voice_478`). Per-run
  `voice_overrides` (`{"Character": "voice_591"}`) win over auto-assignment.
- Segmentation attributes dialogue per chapter using that global cast.

## Data sources and identity

- **Google Drive** via the `google_drive_mcp` connector slot (`@uses_connectors`)
  under the triggering user's credential (OBO; hardened deployment). If the user
  has no credential yet, the run pauses with an OAuth `auth_url` (10-minute
  window). The epub is only read; outputs are written only into the output folder.
- **Mistral API** (`chat.complete` for scripting, `audio.speech.complete` for
  TTS) via `get_mistral_client()` — the deployment's service account; no
  `MISTRAL_API_KEY` secret is needed on the managed deployment (locally, set
  `MISTRAL_API_KEY`).
- **ffmpeg/ffprobe** in the worker image for concat/split/duration.

## Input

```json
{
  "epub_file_id": "1AbC...",
  "output_folder_id": null,
  "narrator_voice": "voice_478",
  "voice_overrides": {},
  "max_characters": 8,
  "script_model": "mistral-large-latest",
  "tts_model": "voxtral-mini-tts-3-0",
  "response_format": "mp3",
  "max_epub_bytes": 8000000,
  "max_book_chars": 1500000,
  "max_merged_bytes": 30000000,
  "max_part_bytes": 12000000,
  "chunk_chars": 3000,
  "batch_chars": 20000,
  "tts_concurrency": 4
}
```

Only `epub_file_id` is required.

## Output

`EpubAudiobookOutput`: `book_title`, `output_folder_id`, `cast[]`, `chapters[]`
(per-chapter files + duration), `files[]` (Drive `view_url`s for chapter MP3s,
merged MP3, `*-script.md`, `*-manifest.json`), `stats`, `warnings`.

## Deploy

Managed deployment **`epub-audiobook-worker`**, **hardened=true** (required for
OBO), `github_url` = this repo, `dockerfile_path` =
`deploy/docker/Dockerfile.epub-voxtral-audiobook`, build context = repo root:

~~~bash
gh repo clone ThomsenDrake/dt-personal-workflows /workspace/github__ThomsenDrake__dt-personal-workflows
# push a commit on main, then:
# deploy_workflow_deployment(deployment_name="epub-audiobook-worker",
#   github_url="https://github.com/ThomsenDrake/dt-personal-workflows",
#   revision="<commit SHA on main>", build_directory=".",
#   dockerfile_path="deploy/docker/Dockerfile.epub-voxtral-audiobook", hardened=true)
~~~

The Mistral GitHub App must be installed on the repo for the build.

Run it from Vibe with the bundled skill (`skill/SKILL.md`,
`/epub-voxtral-audiobook <drive link or file id>`), or:

~~~bash
export MISTRAL_API_KEY=...
python workflows/epub-voxtral-audiobook/examples/run_execution.py
~~~

## Limitations

- Cost and time scale with book length (LLM per batch + per chapter; Voxtral TTS
  billed per character). Long novels run for hours; activities have hour-scale
  timeouts and Temporal retries.
- Temporal/connector payload caps: epubs over `max_epub_bytes` (8 MB) and books
  over `max_book_chars` (1.5 M chars) fail early with a clear error.
- The merged MP3 is skipped above `max_merged_bytes` (30 MB); chapter files above
  `max_part_bytes` (12 MB) are split into `-partNNN` files.
- Voice catalog defaults to English presets; other languages work via Voxtral's
  multilingual presets passed through `narrator_voice`/`voice_overrides`.
- You must hold the rights to convert the book's text.

## Layout (this workflow's slice of the monorepo)

~~~
workflows/epub-voxtral-audiobook/
  worker/
    pyproject.toml
    src/worker/entrypoints/worker.py      # worker entrypoint
    src/worker/workflows/epub_audiobook.py  # workflow class + activities (SDK glue)
    src/worker/workflows/models.py        # pydantic input/output
    src/worker/workflows/epub_parse.py    # pure-stdlib epub parsing
    src/worker/workflows/cast_logic.py    # pure casting/chunking/prompts
    tests/                                # pytest, no network
  examples/run_execution.py + sample_input.json
  skill/SKILL.md                          # Vibe slash-command skill
~~~
