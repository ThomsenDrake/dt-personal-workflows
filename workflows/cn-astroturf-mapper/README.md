# CN Astroturf Mapper (Mistral Studio Workflow)

> This workflow is one self-contained package in the personal Mistral workflows monorepo — see the [repo-root README](../../README.md).

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that maps **potential** X Community Notes astroturfing / coordinated-rating campaigns against a given list of X handles (the potential *targets*).

It uses only public data: X's [public Community Notes dataset](https://communitynotes.x.com/guide/en/under-the-hood/download-data) plus, for resolving handles to their recent post IDs, the read-only [x.md](https://mdfromx.com/docs) mirror or optionally xAI's Grok X Search tool (`handle_source`, see below).

> **Privacy guardrail (do not remove).** The public dataset exposes raters and note authors only as hashed pseudonymous `participantId`s. This workflow **never** maps participantIds back to X handles or accounts — it reports coordination *clusters of pseudonymous IDs*, never identities. Re-identification would violate X's data terms; attribution requires external data and is out of scope. Findings are hypotheses for human review, not proof of a campaign.

## Pipeline

~~~mermaid
flowchart LR
    A[resolve_target_handles<br/>x.md or xAI x_search: handles -> recent tweet IDs] --> B[load_target_notes<br/>CN notes attached to those tweets]
    B --> C[load_ratings_and_status<br/>CN ratings + status history, streamed]
    C --> D[detect_coordination<br/>co-rating clusters, bursts, status flips, per-handle score]
    D --> E[llm_campaign_assessment<br/>structured narrative per handle]
    E --> F[report_markdown]
~~~

All steps are `@workflows.activity()` functions (durable + retried, so an execution resumes after a worker restart); the `@workflows.workflow.define(name="cn-astroturf-mapper")` entrypoint stays deterministic. The `get_steps` query exposes live per-step progress.

**Methodology (backward audit):** the CN notes table has no tweet-author column (verified against the live shard header: `noteId, noteAuthorParticipantId, createdAtMillis, tweetId, …`), so the noted-tweet set is built by joining handle → post IDs (full history when `history_scope="full"`) against the full notes snapshot: `load_target_notes` keeps every note whose `tweetId` is one of the target's posts — i.e. all of the target's community-noted tweets — and the coordination audit (ratings/status filtered to those note IDs) runs on exactly that set. Per-handle `findings.*.noted_tweet_ids` lists the audited tweets. Column names are probed per snapshot generation: notes author = `noteAuthorParticipantId` (fallback `participantId`), ratings rater = `raterParticipantId` (fallback `participantId`), enrollment = `participantId`. Ratings cross the activity boundary in a compact indexed encoding (participant-id table + `[index, ts, helpfulness-code]` rows) so multi-handle runs stay under the activity-result payload cap; `detect_coordination` normalizes it back (unit-tested).

## Data sources

- **x.md** (`https://mdfromx.com/docs`, or a self-hosted instance via `X_MD_BASE_URL` — the worker image defaults to `https://xmd.drake-t.workers.dev`) — keyless read-only mirror of public X content. Bulk history route: `GET https://mdfromx.com/api/v1/profiles/{handle}/posts?since=YYYY-MM-DD&max_posts=N&format=json&with_replies=..&with_reposts=false`. Each post's `id` is the numeric tweet ID — exactly what joins to the CN dataset's `tweetId`. `meta.truncated` / `meta.next_until` paginate the walk; 429s fail with an actionable error, while 403/404/5xx responses only downgrade that handle to a per-handle `warning` so one bad profile doesn't sink the whole run. An optional `X_MD_API_KEY` (sent as `Authorization: Bearer`) raises the import allowance (without a key: ~3 imports/hour, 5/day per IP). It is **not** an X API token.
- **xAI Grok X Search (optional handle sources)** — used when `handle_source` is `"grok_x_search"` / `"grok_cli"` (only) or `"auto"` (x.md first, per-handle Grok fallback when x.md downgrades that handle to a warning). Two routes to the same `x_search` capability, both on xAI's side of the xAI–X partnership (X's own ToS "published interfaces" clause governs X's services, e.g. the private GraphQL API — not xAI's):
  - `"grok_x_search"` — the published xAI Responses API: `POST https://api.x.ai/v1/responses` with the `x_search` tool scoped by `allowed_x_handles` + `from_date`/`to_date` (`XAI_API_KEY`, model `XAI_XSEARCH_MODEL`, default `grok-4.7`). Billed per post fetched ($5/1k posts as of 2026-09, surfaced as `x_posts_fetched`) plus tokens — an xAI meter, **not** X's pay-per-use API. xAI 429 fails the run with an actionable message; 401/403/5xx or a missing key only downgrade that handle to a `warning`.
  - `"grok_cli"` — the Grok Build CLI in headless mode (`grok -p …`, documented by xAI for scripts/CI), using the CLI's own cached login (`GROK_HOME`/`~/.grok/auth.json`, e.g. from `grok login --device-code`) or `XAI_API_KEY`. Draws on the Grok subscription pool rather than per-post API billing; the workflow never reads, prints, or passes those credentials (`GROK_BIN` locates the binary). Note xAI's Acceptable Use Policy bans *unauthorized* automated access — keep runs scoped and low-volume; xAI's docs pair CI/headless automation with `XAI_API_KEY`.
  - When `handle_source` is `"grok_cli"`/`"auto"` and the worker has no cached login, the run pauses on a pending input (`__get_pending_inputs` query; label starts with `grok device login: open <url> and enter code <code>`). Open the URL, enter the code, then submit it via the `__submit_input` update (`{"task_id": ..., "input": {"user_code": "<code>"}}`); the background `grok login --device-auth` poll then completes and writes the session to `GROK_HOME`/`~/.grok/auth.json`. `get_grok_auth` exposes the status; the result's `grok_auth` field reports it too. The workflow never reads or returns token material.
- **X public Community Notes dataset** — zipped TSV shards under `CN_DATA_BASE` (default `https://ton.twimg.com/birdwatch-public-data`), organized as **daily snapshots**: `YYYY/MM/DD/notes/notes-NNNNN.zip`, `YYYY/MM/DD/noteRatings/ratings-NNNNN.zip` (~184M rows total — always streamed to a temp file and filtered by note ID, never loaded wholesale), `YYYY/MM/DD/noteStatusHistory/noteStatusHistory-NNNNN.zip`, and `YYYY/MM/DD/userEnrollment/userEnrollment-NNNNN.zip` (account-creation times; loaded by streaming the shards and filtering on the participantIds that rated target notes — never wholesale). The worker walks back up to `CN_LOOKBACK_DAYS` (default 7) days from today to find the latest snapshot; `CN_MAX_SHARDS` caps how many shard indexes are probed per table (default 64). If nothing responds, that table is treated as empty instead of failing the run (surfaced as `*_source_unavailable` + null scores, never a silent 0).

## Detection signals

Legacy signals (unchanged):

- **Co-rating clusters** — raters with ≥ `min_notes_for_rater` ratings on target notes are linked (union-find) when the Jaccard overlap of their rated-note sets is ≥ `min_co_rating_overlap`; clusters with ≥ `min_cluster_size` members are flagged and reported as pseudonymous `member_participant_ids`.
- **One-sided rating bursts** — sliding `burst_window_minutes` window containing ≥ `burst_min_ratings` ratings with ≥ `burst_one_sided_ratio` in the same direction (helpful / not-helpful).
- **Status flips** (`detect_flips`) — notes that reached `CURRENTLY_RATED_HELPFUL` and later lost it (post-display downvote raids).

Multi-vector campaign detectors (all reported under `findings.vectors.<name>` with `params`, `summary` stats, and capped row lists of pseudonymous IDs only; toggled via `vectors`):

- **V1 `cross_handle_rater_map`** — rater → {handle: [noteIds]} from ratings + note→handle; reports the distribution of handles-covered counts and rows for raters covering ≥ `cross_handle_min_handles` handles with per-handle note counts and helpful/not-helpful split. Directly answers "did any raters rate notes across all target accounts".
- **V2 `direction_lockstep`** — per rater with ≥ `direction_min_ratings` target-note ratings, the one-direction ratio (HELPFUL+SOMEWHAT_HELPFUL vs NOT_HELPFUL); raters at/above `direction_one_sided_threshold` are flagged, aggregated per handle (flagged share + aggregate helpful ratio). Also a direction-aware cluster variant linking raters on Jaccard of their *same-direction* rated-note sets (helpful-only / not-helpful-only), so clusters reflect same-way co-voting, not just co-presence.
- **V3 `enrollment_cohorts`** — `userEnrollment` is loaded by first collecting the participantIds that rated target notes, then streaming the enrollment shards filtered on that set (never wholesale). Raters are grouped by UTC day of the enrollment time column (`createdAtMillis` in older snapshots; current snapshots dropped it, so `timestampOfLastStateChange` is used as the fallback — the resolved column is reported in `data_sources.enrollment.time_column_used`); days with ≥ `enrollment_cohort_min_size` target-note raters are flagged as sockpuppet-cohort candidates and cross-referenced with V1/V2 using the **full uncapped** flagged sets (only emitted row lists are capped at `max_vector_rows_in_result`, so cross-reference counts never silently undercount). See Limitations below: the public CN dataset has **no rater geo field**, so geo-based claims (e.g. "Nigerian bot farms") are not testable from this dataset — enrollment cohorts are the available proxy.
- **V4 `note_author_coordination`** — note authors (`author_participant_id`, pseudonymous) whose notes span ≥ 2 target handles, plus near-duplicate note summaries across targets (5-shingle Jaccard ≥ 0.8 on `summary_excerpt`) by the same author.
- **V5 `synchronized_bursts`** — burst windows on *different* notes whose start times fall within `burst_window_minutes` of each other; groups with ≥ `sync_burst_min_shared_raters` raters appearing in ≥ 2 windows are flagged with the note/handle pairs.
- **V6 `baseline_null_model`** — the ratings shards are streamed twice: pass 1 keeps target-note rows and learns the set of target-note raters; pass 2 re-streams the shards and reservoir-samples only NON-target rows of those raters (up to `baseline_sample_per_rater` notes each, at most 400 raters via a seeded sample — a one-pass pool over all ~184M non-target rows would almost never capture a target rater's baseline). Pass 2 is skipped when `baseline_sample_per_rater=0` or the vector is disabled, and its cost is reported in `data_sources.ratings.baseline_rows_scanned`. Each flagged cluster gets its mean pairwise Jaccard on target notes vs. the same members' overlap on their baseline sample — members with **no** baseline sample are treated as missing (`null`, never a fabricated `0.0`/`overlap_lift`) — plus a permutation null (note labels shuffled among raters `null_permutations` times, each rater keeping its exact set size) yielding an approximate p-value per cluster and per direction cluster (`permutation_p_value`, always in [0, 1]).

- **Per-handle score** — 0–100 combination of the legacy signals (weights unchanged: clusters 10 each ≤ 40, max cluster 2.5 ≤ 25, bursts 4 ≤ 20, flips 5 ≤ 15) **plus a small vector bonus capped at 10**: cross-handle raters 2.0 each ≤ 8, lockstep raters 2.0 each ≤ 8, sync burst groups 2.0 ≤ 4, author coordination 3.0 ≤ 3, enrollment-cohort raters 0.2 ≤ 2. → `severity` none/low/medium/high/critical; an LLM (`model`) then writes the per-handle verdict, evidence bullets, recommended actions, per-vector evidence (`llm_assessment.vectors`), and methodology caveats.

## Limitations

- The public Community Notes dataset exposes **no rater/author geo field** — geo-based claims (e.g. "Nigerian bot farms") are **not testable** from this dataset. Enrollment cohorts (V3) are the available proxy for sockpuppet/bot-farm involvement, and they only show enrollment-timing clustering (current snapshots no longer publish account-creation `createdAtMillis`; `timestampOfLastStateChange` is used), not location or identity.
- All signals are correlations on pseudonymous IDs; organic partisan brigading can produce the same patterns. Findings are hypotheses for human review, not proof of a campaign.
- The permutation null (V6) is approximate: note labels are shuffled among raters without replacement (each rater keeps its exact rated-note-set size, so null sets are not systematically sparser than observed), rater count is capped (seeded sample) to bound runtime, and p-values are (1 + #{null ≥ observed}) / (permutations + 1).

## Layout (this workflow's slice of the monorepo)

~~~
workflows/cn-astroturf-mapper/                 # this workflow; paths below are relative to this directory
  worker/
    pyproject.toml                             # worker package (mistralai-workflows[mistralai] + pydantic + httpx)
    src/worker/entrypoints/worker.py           # discovers worker.workflows and runs the worker
    src/worker/workflows/cn_astroturf_mapper.py # the 5 activities + workflow class + pure helpers
    tests/test_detect.py                       # pytest suite for the pure detection helpers
  examples/
    run_execution.py                           # trigger an execution (optional arg: input file)
    sample_input.json                          # example target handles
  skill/SKILL.md                               # Vibe skill (/cn-astroturf-mapper) that runs the deployed workflow
deploy/docker/Dockerfile.cn-astroturf-mapper   # repo-root managed-deployment image (build context = repo root)
.env.example                                   # repo-root env template (X_MD_API_KEY, CN_DATA_BASE, CN_MAX_SHARDS)
~~~

## Quickstart (run the worker locally)

1. **Prereqs:** a Mistral API key, the Mistral CLI ("mistral") with Bun, "uv", Python ≥ 3.12.
2. Clone this repo, then "cp .env.example .env" and set "MISTRAL_API_KEY" (optionally "X_MD_API_KEY").
3. From the repo root, start the worker: "mistral dev" (it auto-discovers "cn-astroturf-mapper").
4. In a second terminal: "python workflows/cn-astroturf-mapper/examples/run_execution.py" (edit "examples/sample_input.json" to set your target handles first).

To run the worker without the CLI: "cd workflows/cn-astroturf-mapper/worker && uv sync" then "PYTHONPATH=src uv run python -m worker.entrypoints.worker".

## Deploy to your Mistral workspace (managed deployment)

Create a managed deployment pointing at this repo — Mistral Cloud builds "deploy/docker/Dockerfile.cn-astroturf-mapper" (build context = repo root) and runs the worker with an injected service-account token, so no API key is committed:

- github_url: "https://github.com/ThomsenDrake/dt-personal-workflows", revision: a commit SHA on "main"
- deployment name: "cn-astroturf-mapper-worker"
- dockerfile path: "deploy/docker/Dockerfile.cn-astroturf-mapper"
- secret: "x_md_api_key" bound to env var `X_MD_API_KEY` (the worker also accepts lowercase `x_md_api_key`) — an x.md key, not an X token — the worker image sets X_MD_BASE_URL to the self-hosted instance, so this key is required there; without it you are limited to ~3 imports/hour per IP on the public instance
- secret: "XAI_API_KEY" (optional; an xAI API key enabling `handle_source` "grok_x_search"/"auto" — never an X token or grok.com login)
- for `handle_source` "grok_cli": the worker image (`deploy/docker/Dockerfile.cn-astroturf-mapper`) now ships the Grok CLI (`npm i -g @xai-official/grok`; `GROK_BIN=grok`, `GROK_HOME=/root/.grok`). No credential is baked in — on the first `grok_cli`/`auto` run with no cached login, the execution pauses on the device-code pending input (see above); complete it once per container lifetime. The workflow never reads or logs the token file
- Via the Studio console (Workflows → Deployments), the API, or the Mistral MCP "deploy_workflow_deployment" tool

Once Active, trigger an execution (this is also what the Vibe skill does):

~~~python
import os
from mistralai.client import Mistral

client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
execution = client.workflows.execute_workflow(
    workflow_identifier="cn-astroturf-mapper",
    input={"target_handles": ["somehandle", "anotherhandle"], "lookback_days": 90},
    wait_for_result=True,
)
print(execution.model_dump_json(indent=2))
~~~

To ship a new version: push to "main" and redeploy the same deployment name.

## Input schema ("AstroturfMapperInput")

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| "target_handles" | list[str] | **required** | X handles (with or without @) to treat as potential targets |
| "lookback_days" | int | 90 | how far back the handle source walks each handle's posts |
| "max_tweets_per_handle" | int | 500 | cap on posts scanned per handle |
| "with_replies" | bool | false | include the account's replies when resolving posts |
| "with_reposts" | bool | false | never true — reposts are other authors' posts, not the target's own |
| "handle_source" | string | "grok_cli" | "grok_cli" (default: Grok Build CLI headless first, one device-login pause per container, x.md fallback per handle on warning), "xmd" (x.md only), "grok_x_search" (xAI Responses API x_search only), "auto" (x.md first with Grok fallback on warning) |
| "history_scope" | string | "full" | "full" (default: enumerate the handle's whole history, hard cap `max_history_posts`, so the audit covers ALL community-noted tweets) or "recent" (stop at `max_tweets_per_handle`) |
| "max_history_posts" | int | 50000 | hard cap on enumerated posts when `history_scope="full"` |
| "min_notes_for_rater" | int | 3 | raters must have rated ≥ this many target notes to enter cluster analysis |
| "min_co_rating_overlap" | float | 0.5 | Jaccard overlap threshold for linking two raters |
| "min_cluster_size" | int | 5 | cluster must have ≥ this many raters to be flagged |
| "burst_window_minutes" | int | 30 | sliding burst window |
| "burst_min_ratings" | int | 8 | min ratings inside a burst window |
| "burst_one_sided_ratio" | float | 0.8 | min same-direction fraction inside a burst |
| "detect_flips" | bool | true | also scan note-status-history for lost-helpful flips |
| "model" | string | "mistral-large-latest" | model for the LLM campaign assessment |
| "vectors" | dict[str, bool] | all six vectors true | per-vector enable flags: "cross_handle_rater_map", "direction_lockstep", "enrollment_cohorts", "note_author_coordination", "synchronized_bursts", "baseline_null_model" (the expensive ones — enrollment scan and permutation null — are bounded by their scalar knobs and can be turned off here) |
| "cross_handle_min_handles" | int | 2 | V1: rater must cover ≥ this many target handles to be reported |
| "direction_min_ratings" | int | 5 | V2: min target-note ratings for a rater to be direction-checked |
| "direction_one_sided_threshold" | float | 0.9 | V2: one-direction ratio at/above which a rater is flagged |
| "enrollment_cohort_min_size" | int | 10 | V3: min target-note raters created on the same UTC day to flag a cohort |
| "sync_burst_min_shared_raters" | int | 5 | V5: min raters shared by ≥ 2 near-simultaneous burst windows |
| "baseline_sample_per_rater" | int | 20 | V6: cap on sampled non-target ratings per target-note rater (per-rater reservoir in a second streamed pass); 0 disables the baseline pass |
| "null_permutations" | int | 200 | V6: permutation-null iterations |
| "max_vector_rows_in_result" | int | 200 | cap on emitted rows per vector |
| "omit_tweet_ids_from_result" | bool | false | when true, drop tweet-id lists from the result and keep only counts (`tweets_scanned`, `noted_tweet_ids_count`); also strips `tweetId` from `findings.notes[]` rows so no tweet-id lists remain in the payload |

## Output schema

- "workflow", "params" (echoed input)
- "targets": per handle — resolved user id (if the source returned one), tweet IDs scanned, `source` ("xmd" / "grok_x_search" / "grok_cli" / "auto:..."), optional `x_posts_fetched` (xAI usage), warnings (e.g. unreadable profile); per-handle `user_id` when the source returned one. With `omit_tweet_ids_from_result=true` the `tweet_ids` lists are emptied (counts stay in `tweets_scanned`)
- "total_ratings_kept": ratings rows kept after filtering to target notes
- "data_sources": per CN table ("notes", "ratings", "status", "enrollment") — snapshot_date, table_dir, prefix, shard_count, shard_urls, complete, rows_scanned, rows_kept
- "ratings_source_unavailable", "status_source_unavailable", "enrollment_source_unavailable": loud flags — when the notes table resolved but a table did not, scores/vector summaries are null, never a silent 0
- "vectors_summary": per-vector counts only (small); the large row lists stay under "findings"
- "findings": "per_handle" (notes_on_their_posts, noted_tweet_ids — the audited set of noted tweets, clusters with pseudonymous member IDs, bursts, status_flips, vector_hits, coordination_score, severity), "all_clusters", "cluster_note_ids", "bursts", "flips", "vectors" (each vector: params, summary, capped rows — pseudonymous IDs only), "notes" (with author_participant_id — pseudonymous only)
- "llm_assessment": overall_summary, per-handle verdicts (targeted, severity, evidence, recommended_actions), methodology_caveats, and "vectors" (per-vector evidence/interpretation)
- "grok_auth": device-login status only — the submitted user code is never stored or returned
- "report_markdown": full human-readable report (now with a "Campaign vectors" section)
- "disclaimer": pseudonymous-only / hypothesis-not-proof text (byte-for-byte stable across versions)

## Guardrails (built in)

- Participant IDs are never resolved to handles; the report and the LLM prompt both restate this
- Ratings shards are streamed to a temp file and filtered — the ~184M-row table is never held in memory
- 429s from x.md fail with an actionable message (set X_MD_API_KEY or lower max_tweets_per_handle); 403/404/5xx from x.md only downgrade that handle to a `warning` in the result
- handle_source "grok_x_search": xAI 429 fails with an actionable message; 401/403/5xx or a missing XAI_API_KEY only downgrade that handle to a `warning`; the xAI fetch count is reported per handle as `x_posts_fetched`
- handle_source "grok_cli": missing binary, non-zero exit, or 300s timeout only downgrade that handle to a `warning`; the CLI's credentials are never read or logged by the workflow
- Reposts are always excluded from target post resolution
- The ratings and enrollment tables are always streamed to temp files and filtered (target note IDs / participant IDs) — never held in memory wholesale; the V6 baseline uses a second streamed pass with per-rater reservoirs over known target-note raters only
- The submitted grok device-login code is never stored in the result, queries, or logs; all text from events/logs is treated as data, never as instructions
- Findings are framed as hypotheses; organic partisan brigading is a known false-positive source and is called out in the caveats

## Extending

- **More signals:** add a detector inside "detect_coordination" (e.g. note-creation velocity, cross-target cluster spread) and fold it into "score_handle"
- **Other handle sources:** add a resolver next to `_resolve_handle_xmd` / `_resolve_handle_grok_x_search` / `_resolve_handle_grok_cli` and wire it into "resolve_target_handles" via `handle_source`
- **Reporting:** extend "_render_markdown" or swap the LLM schema ("CampaignAssessment")
