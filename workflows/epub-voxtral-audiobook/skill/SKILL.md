---
name: |
  epub-voxtral-audiobook
description: |
  Run the deployed `epub-voxtral-audiobook` Mistral Studio workflow on the `epub-audiobook-worker` deployment to turn a .epub from Google Drive into a multi-character casted audiobook synthesized with Voxtral (per-chapter MP3s + merged MP3 + cast script + manifest, uploaded back to Drive). Load this skill whenever the user asks to convert an epub/ebook into an audiobook, make a "full cast" audiobook, cast voices for a book with Voxtral, or says to run the epub-voxtral-audiobook workflow.
---

# EPUB → Voxtral Audiobook Runner

When the user wants to turn a .epub into a multi-character audiobook, run the
managed workflow deployment in the Mistral Studio workspace and present its
output. The workflow runs **on behalf of the user** (`on_behalf_of=True`), so it
reads and writes **their** Google Drive through their `google_drive_mcp`
credential — no secrets are handled here.

## What to run

Use the connected Mistral connector (`mistral_mcp`) tools:

1. Start the execution:
   - `execute_workflow` with
     - `workflow_identifier`: `epub-voxtral-audiobook`
     - `deployment_name`: `epub-audiobook-worker`
     - `input`: see "Input" below
     - `_confirmationReason`: short reason, e.g. "User asked to convert the epub <title/file> into a multi-character Voxtral audiobook in their Drive."
   - (SDK equivalent: `client.workflows.execute_workflow(workflow_identifier="epub-voxtral-audiobook", deployment_name="epub-audiobook-worker", input={...})`)
2. Poll `get_workflow_execution` with the returned `execution_id` until
   `status` is `COMPLETED` or `FAILED` (between polls, optionally call
   `read_workflow_execution_events` with `wait_seconds` ~10–20 for progress, and
   `query_workflow_execution` with `query_name` `get_steps` for the step map).
   On `FAILED`, read `get_workflow_execution_logs` and report the failure, then stop.
   These runs can take a long time on full novels (TTS is billed per character and
   the whole book is scanned for characters) — keep polling; do not give up early.
3. First run per user may pause on a connector OAuth preflight: an event /
   pending input carries an `auth_url` for the `google_drive_mcp` connector.
   Tell the user to open the URL and authorize Google Drive, then the run
   resumes by itself (10-minute window).

## Input

```json
{
  "epub_file_id": "1AbC...",           // required: Drive file ID of the .epub
  "output_folder_id": null,             // optional: existing Drive folder; default = epub-audiobook-output/<book title>
  "narrator_voice": "voice_478",        // Voxtral voice_id for the narrator (default voice_478 / Sheila, en-US)
  "voice_overrides": {"Alice": "voice_447"},  // optional: character name -> voice_id, wins over auto-cast
  "max_characters": 8,                  // distinct voiced characters cap; the rest fold into the narrator
  "script_model": "mistral-large-latest",     // LLM for the full-book character scan + segmentation
  "tts_model": "voxtral-mini-tts-3-0",        // Voxtral model for POST /v1/audio/speech
  "response_format": "mp3",
  "max_epub_bytes": 8000000,
  "max_book_chars": 1500000,
  "max_merged_bytes": 30000000,         // merged MP3 is skipped above this size (per-chapter files still upload)
  "chunk_chars": 3000,                  // max chars per single TTS request
  "batch_chars": 20000                  // chars per batch in the full-book character scan
}
```

Map user phrasing onto `input`:

- A Drive link (`https://drive.google.com/file/d/<ID>/view...`), `?id=<ID>`,
  `open?id=<ID>`, or a bare file ID → `epub_file_id`. If the user names a file
  by title instead, resolve it first with the `google_drive_mcp` connector's
  `search_files` (`query`: `title contains '<name>' and mimeType contains 'epub'`,
  or `title = '<exact>'`) and use the returned `id`. If no epub is named, ask
  which Drive file to convert before running.
- "use voice X for the narrator" → `narrator_voice` (any preset slug/ID, e.g.
  `gb_jane_neutral` or `voice_485`).
- "Alice should sound like <voice>" / "cast Bob as voice_591" →
  `voice_overrides: {"Alice": "<voice>", "Bob": "voice_591"}`.
- "only N characters get their own voice" → `max_characters`.
- "write outputs to folder <id>" → `output_folder_id`.
- "shorter/longer chapters in the audio" → chapter files follow the epub's own
  chapter structure; there is no re-chunking knob beyond `chunk_chars` (TTS
  request size) — do not invent one.

The character scan covers the **entire book** (batched), not just the opening
chapters. Preset English voices: `voice_478` Sheila, `voice_447` Margaret,
`voice_485` Rob, `voice_3150` James (en-US); `voice_1787` Mary, `voice_1786` Sky,
`voice_1788` David, `voice_590` Harriet, `voice_591` Fred, `voice_3149` Hugo,
`voice_3147` Howard (en-GB); Voxtral 2 slugs like `gb_jane_neutral` also work.

## Output

Summarize for the user from `result`:

- `result.book_title`, `result.output_folder_id` (open it in Drive), and
  `result.files`: every uploaded file with `name`, `view_url`, `size_bytes` —
  per-chapter MP3s (`<title>-chNNN[-partNNN].mp3`), the merged MP3
  (`<book>-merged.mp3`, present unless over `max_merged_bytes`), the cast script
  (`<book>-script.md`), and the manifest (`<book>-manifest.json`).
- `result.cast`: character → `voice_id`/`voice_name` table (narrator first).
- `result.chapters`: per-chapter segment counts and `duration_seconds`.
- `result.stats`: chapters, segments, book chars, TTS chars, scan batches.
- `result.warnings`: truncation, narrator-folded characters, unknown overrides,
  skipped merged file, etc. — always surface these.

## Troubleshooting

- Deployment not ready → `get_workflow_deployment` for `epub-audiobook-worker`
  and report `ready` / `rollout_status` / `active_worker_count` / `is_hardened`.
- Workflow not registered → `list_workflow_registrations` with
  `workflow_name="epub-voxtral-audiobook"`; if absent, the worker code in
  `workflows/epub-voxtral-audiobook/worker` of the dt-personal-workflows
  monorepo must be deployed first (managed deployment `epub-audiobook-worker`,
  `hardened=true`, dockerfile `deploy/docker/Dockerfile.epub-voxtral-audiobook`).
- `FAILED` with an OBO/identity error ("no user identity", "hardened") → the run
  must be started by a signed-in user (Vibe/Studio), and the deployment must be
  hardened; recreate with `deploy_workflow_deployment(hardened=true)`.
- `FAILED` with "not an epub" / size errors → the Drive file ID is wrong or the
  epub exceeds `max_epub_bytes`; resolve the right file via `search_files`.
- OAuth `auth_url` event → see step 3 above; the run waits 10 minutes for the
  user to authorize the `google_drive_mcp` connector.
- `FAILED` with an audio/speech `4xx` (e.g. `invalid_voice`) → a `voice_id` in
  `narrator_voice`/`voice_overrides` is not a valid preset; fix it against the
  voice list above and re-run.
