"""Unit tests for the phase-2 participant-expansion (V7) pure helpers in cn_astroturf_mapper."""
import asyncio
import os
import random

import pytest

import worker.workflows.cn_astroturf_mapper as mod
from worker.workflows.cn_astroturf_mapper import (
    AstroturfMapperInput,
    _augment_assessment_with_expansion,
    _render_markdown,
    build_expansion_seeds,
    build_participant_expansion_vector,
    detect_coordination,
    expansion_pass_a,
    expansion_pass_b,
    expansion_seed_note_majority,
    participant_tier,
    run_participant_expansion,
    same_direction_jaccard,
    score_participant,
)


def _row(nid, pid, level="HELPFUL"):
    return {"noteId": nid, "raterParticipantId": pid, "helpfulnessLevel": level}


# ── seed set ──


def test_build_expansion_seeds_unions_all_flag_sources():
    seeds = build_expansion_seeds(
        {"v1a", "v1b"}, {"v2a"}, {"d1", "d2"}, {"c1"}, {"b1", "b2"}, {"m1", "m2"}, seed_max=100,
    )
    assert seeds == sorted({"v1a", "v1b", "v2a", "d1", "d2", "c1", "b1", "b2", "m1", "m2"})


def test_build_expansion_seeds_cap_is_seeded_and_deterministic():
    big = {f"p{i:04d}" for i in range(100)}
    a = build_expansion_seeds(big, None, None, None, None, None, seed_max=10)
    b = build_expansion_seeds(big, None, None, None, None, None, seed_max=10)
    assert a == b and len(a) == 10 and set(a) <= big
    # None sources are tolerated (vector disabled upstream)
    assert build_expansion_seeds(None, None, None, None, None, None, seed_max=10) == []


# ── pass A bookkeeping ──


def test_pass_a_records_seed_notes_and_directions():
    rows = [
        _row("n1", "s1", "HELPFUL"),
        _row("n1", "s2", "HELPFUL"),
        _row("n2", "s1", "NOT_HELPFUL"),
        _row("n9", "other", "HELPFUL"),  # non-seed rater: ignored in pass A
        _row("n1", "s1", "HELPFUL"),     # duplicate (seed, note): counted once
    ]
    res = expansion_pass_a(rows, {"s1", "s2"}, rater_note_cap=10, note_fanin_cap=5)
    assert res["seed_notes"] == {"n1", "n2"}
    assert res["seed_shared"] == {"s1": 2, "s2": 1}
    assert res["note_agg"]["n1"] == {"raters": {"s1", "s2"}, "helpful": 2, "not_helpful": 0}
    assert res["note_agg"]["n2"]["not_helpful"] == 1
    assert res["fanin_excluded"] == 0
    assert set(res["seed_note_lists"]["s1"]) == {"n1", "n2"}


def test_pass_a_note_fanin_cap_excludes_mega_notes():
    rows = [_row("n1", f"s{i}", "HELPFUL") for i in range(4)] + [_row("n2", "s0", "HELPFUL")]
    res = expansion_pass_a(rows, {f"s{i}" for i in range(4)}, rater_note_cap=10, note_fanin_cap=3)
    assert "n1" not in res["seed_notes"] and res["seed_notes"] == {"n2"}
    assert res["fanin_excluded"] == 1
    # the excluded note's contribution is rolled back: s0 also rated n2 (kept), the rest drop to 0
    assert res["seed_shared"] == {"s0": 1, "s1": 0, "s2": 0, "s3": 0}


def test_pass_a_per_seed_reservoir_cap_is_bounded_and_deterministic():
    rows = [_row(f"n{i}", "s1", "HELPFUL") for i in range(50)]
    r1 = expansion_pass_a(rows, {"s1"}, rater_note_cap=5, note_fanin_cap=100,
                          rng=random.Random(mod.EXPANSION_RNG_SEED))
    r2 = expansion_pass_a(rows, {"s1"}, rater_note_cap=5, note_fanin_cap=100,
                          rng=random.Random(mod.EXPANSION_RNG_SEED))
    assert len(r1["seed_note_lists"]["s1"]) == 5
    assert r1["seed_note_lists"] == r2["seed_note_lists"]  # seeded => reproducible
    # the shared count follows the per-seed recording cap (bounded note_agg memory)
    assert r1["seed_shared"]["s1"] == 5


def test_pass_a_per_seed_cap_bounds_note_agg_memory():
    # a seed rating far more notes than rater_note_cap must not grow note_agg without bound
    rows = [_row(f"n{i}", "s1", "HELPFUL") for i in range(50)] + [_row("shared", "s2", "HELPFUL")]
    res = expansion_pass_a(rows, {"s1", "s2"}, rater_note_cap=5, note_fanin_cap=100)
    assert res["seed_shared"]["s1"] == 5                      # per-seed recording capped
    assert len(res["note_agg"]) == 6                           # 5 notes of s1 + the shared one
    assert res["seed_shared"]["s2"] == 1
    # determinism of the capped recording under the seeded reservoir
    again = expansion_pass_a(rows, {"s1", "s2"}, rater_note_cap=5, note_fanin_cap=100,
                             rng=random.Random(mod.EXPANSION_RNG_SEED))
    assert again["seed_note_lists"] == res["seed_note_lists"]


def test_seed_note_majority_direction():
    agg = {
        "n1": {"raters": {"a", "b"}, "helpful": 2, "not_helpful": 0},
        "n2": {"raters": {"a"}, "helpful": 0, "not_helpful": 1},
        "n3": {"raters": {"a", "b"}, "helpful": 1, "not_helpful": 1},
    }
    assert expansion_seed_note_majority(agg) == {"n1": "helpful", "n2": "not_helpful", "n3": "other"}


# ── pass B bookkeeping ──


def test_pass_b_counts_shared_seed_notes_with_direction_and_matches():
    seed_notes = {"n1", "n2"}
    majority = {"n1": "helpful", "n2": "not_helpful"}
    rows = [
        _row("n1", "p1", "HELPFUL"),
        _row("n2", "p1", "NOT_HELPFUL"),
        _row("n1", "p2", "HELPFUL"),
        _row("n3", "p2", "HELPFUL"),                 # not a seed note
        {"noteId": "n1", "raterParticipantId": None, "helpfulnessLevel": "HELPFUL"},  # unknown rater
    ]
    res = expansion_pass_b(rows, seed_notes, majority)
    st = res["rater_stats"]["p1"]
    assert st == {"shared": 2, "helpful": 1, "not_helpful": 1, "match_helpful": 1, "match_not_helpful": 1}
    assert res["rater_stats"]["p2"]["shared"] == 1
    assert None not in res["rater_stats"]  # unknown-rater rows never attributed


# ── same-direction Jaccard + tiers + scoring ──


def test_same_direction_jaccard_math():
    st = {"helpful": 4, "not_helpful": 0, "match_helpful": 3, "match_not_helpful": 0}
    assert same_direction_jaccard(st, seed_helpful_notes=10, seed_not_helpful_notes=0) == round(3 / 11, 4)
    assert same_direction_jaccard({"helpful": 0, "not_helpful": 0, "match_helpful": 0, "match_not_helpful": 0}, 5, 5) == 0.0
    st_n = {"helpful": 0, "not_helpful": 5, "match_helpful": 0, "match_not_helpful": 5}
    assert same_direction_jaccard(st_n, 0, 5) == 1.0


def test_participant_tier_thresholds():
    assert participant_tier(60.0) == "high"
    assert participant_tier(100.0) == "high"
    assert participant_tier(59.9) == "medium"
    assert participant_tier(30.0) == "medium"
    assert participant_tier(29.9) == "low"
    assert participant_tier(0.0) == "low"


def test_score_participant_documented_weights_flags_and_baseline():
    p_note_handle = {"n1": "h1", "n2": "h2"}
    rater_note_dir = {"s1": {"n1": "helpful", "n2": "not_helpful"}}
    st = {"level": 0, "shared": 10, "helpful": 10, "not_helpful": 0, "match_helpful": 10, "match_not_helpful": 0}
    row = score_participant(
        "s1", st, seed_helpful_notes=10, seed_not_helpful_notes=0,
        note_handle=p_note_handle, rater_note_dir=rater_note_dir,
        flag_sets={"v1": {"s1"}, "lockstep": set(), "cohort": set(), "burst": set()},
        baseline_by_rater={"s1": ["b1", "n1"]}, seed_notes={"n1", "n3"},
    )
    # jac = 10/(10+10-10) = 1.0; score = min(30, 2*10) + 20*1.0 + min(16, 4*2) + min(20, 8) + 5 = 61
    assert row["same_direction_jaccard"] == 1.0
    assert row["score"] == 61.0 and row["tier"] == "high"
    assert row["handles_covered"] == ["h1", "h2"] and row["handles_covered_count"] == 2
    assert row["target_note_helpful"] == 1 and row["target_note_not_helpful"] == 1
    assert row["in_v1"] is True and row["in_lockstep"] is False
    assert row["expansion_level"] == 0
    assert row["baseline_sample_size"] == 2 and row["baseline_seed_note_overlap"] == 1
    # no baseline data -> null scatter, not 0
    row2 = score_participant("s1", st, 10, 0, p_note_handle, rater_note_dir, {}, None, {"n1"})
    assert row2["baseline_sample_size"] is None and row2["baseline_seed_note_overlap"] is None


def test_build_participant_expansion_vector_caps_rows_ids_and_summary():
    p = AstroturfMapperInput(target_handles=["h1", "h2"], max_vector_rows_in_result=2, expand_max_participants=2)
    expansion = {
        "seeds": ["s1", "s2"],
        "participants": {
            "s1": {"level": 0, "shared": 9, "helpful": 9, "not_helpful": 0, "match_helpful": 9, "match_not_helpful": 0},
            "s2": {"level": 0, "shared": 1, "helpful": 1, "not_helpful": 0, "match_helpful": 1, "match_not_helpful": 0},
            "p1": {"level": 1, "shared": 3, "helpful": 0, "not_helpful": 3, "match_helpful": 0, "match_not_helpful": 3},
        },
        "seed_notes": {"n1", "n2", "n3"},
        "seed_helpful_notes": 2,
        "seed_not_helpful_notes": 1,
        "levels_run": 1,
        "passes": [{"level": 1, "pass": "A", "rows_scanned": 100}, {"level": 1, "pass": "B", "rows_scanned": 100}],
    }
    note_handle = {"n1": "h1", "n2": "h2", "n3": "h1"}
    rater_note_dir = {"s1": {"n1": "helpful", "n2": "helpful"}, "p1": {"n3": "not_helpful"}}
    vec = build_participant_expansion_vector(
        expansion, note_handle, rater_note_dir,
        {"v1": {"s1"}, "lockstep": {"s1"}, "cohort": set(), "burst": {"p1"}}, {"s1": ["b1"]}, p,
    )
    assert vec["params"]["expand_levels"] == 1 and vec["params"]["expand_max_participants"] == 2
    assert vec["params"]["seed"] == mod.EXPANSION_RNG_SEED
    s = vec["summary"]
    assert s["seeds"] == 2 and s["participants_total"] == 3 and s["seed_notes"] == 3
    assert s["participants_per_level"] == {"0": 2, "1": 1}
    assert s["tier_counts"] == {"high": s["tier_high"], "medium": s["tier_medium"], "low": s["tier_low"]}
    assert sum(s["tier_counts"].values()) == 3
    assert s["max_handles_covered"] == 2
    assert len(vec["rows"]) == 2                      # capped by max_vector_rows_in_result
    assert len(vec["participant_ids"]) == 2           # capped by expand_max_participants
    assert vec["rows"][0]["participantId"] == "s1"    # sorted by -score: the seed with most shared notes first
    assert vec["participant_ids"][0] == "s1"
    assert vec["passes"][0]["pass"] == "A"
    assert all(r["participantId"] for r in vec["rows"])


# ── streamed driver (levels, determinism) ──


def _driver_rows():
    return [
        _row("n1", "s1", "HELPFUL"), _row("n2", "s1", "HELPFUL"),
        _row("n1", "s2", "HELPFUL"),
        _row("n1", "p1", "HELPFUL"), _row("n2", "p1", "HELPFUL"), _row("n3", "p1", "HELPFUL"),
        _row("n1", "p2", "HELPFUL"),                       # shares only 1 seed note at level 1
        _row("n2", "p3", "HELPFUL"), _row("n3", "p3", "HELPFUL"),  # n3 is a seed note only from level 2
    ]


def test_run_participant_expansion_two_levels(monkeypatch):
    async def fake_rows(client, url):
        for r in _driver_rows():
            yield r

    monkeypatch.setattr(mod, "_aiter_tsv_rows", fake_rows)
    p = AstroturfMapperInput(target_handles=["h"], expand_levels=2, expand_min_shared_notes=2)

    async def run():
        return await run_participant_expansion(["fake://0"], ["s1", "s2"], p)

    exp = asyncio.run(run())
    exp2 = asyncio.run(run())
    keys = ("seeds", "seed_notes", "seed_helpful_notes", "seed_not_helpful_notes",
            "levels_run", "passes", "participants_count")
    assert {k: exp[k] for k in keys} == {k: exp2[k] for k in keys}  # deterministic
    parts = dict(mod.iter_expansion_participants(exp))
    assert parts == dict(mod.iter_expansion_participants(exp2))
    assert exp["participants_file"] and exp2["participants_file"]
    os.unlink(exp["participants_file"])
    os.unlink(exp2["participants_file"])
    assert exp["seeds"] == ["s1", "s2"]
    assert exp["levels_run"] == 2 and len(exp["passes"]) == 4  # 2 scans per level
    assert exp["seed_notes"] == {"n1", "n2", "n3"}
    levels = {pid: st["level"] for pid, st in parts.items()}
    assert levels == {"s1": 0, "s2": 0, "p1": 1, "p3": 2}  # p2 never reaches min shared notes
    assert parts["p1"]["shared"] == 3
    assert exp["seed_helpful_notes"] == 3 and exp["seed_not_helpful_notes"] == 0


def test_pass_b_spill_merge_is_bounded_and_exact(tmp_path):
    # with a tiny spill cap the spiller writes several pid-sorted files; the merge must sum
    # counters per rater exactly (peak RAM stays O(spill cap), not O(#raters))
    sn = {"n1", "n2"}
    maj = {"n1": "helpful", "n2": "not_helpful"}
    sp = mod._PassBSpill(sn, maj, spill_raters=2, tmpdir=str(tmp_path))
    rows = [
        _row("n1", "a", "HELPFUL"), _row("n1", "b", "HELPFUL"), _row("n2", "a", "NOT_HELPFUL"),
        _row("n2", "a", "HELPFUL"), _row("n1", "c", "HELPFUL"), _row("n9", "z", "HELPFUL"),
    ]
    for r in rows:
        sp.update(r)
    files, _ = sp.finish()
    assert len(files) >= 2
    merged = dict(mod._merge_passb_files(files))
    assert merged["a"] == [3, 2, 1, 1, 1]
    assert merged["b"] == [1, 1, 0, 1, 0]
    assert merged["c"] == [1, 1, 0, 1, 0]
    assert "z" not in merged  # n9 is not a seed note
    for f in files:
        os.unlink(f)


# ── detect_coordination wiring ──


def _detect_fixture():
    p = AstroturfMapperInput(target_handles=["h1", "h2"], min_notes_for_rater=2, min_cluster_size=5,
                             direction_min_ratings=2, burst_min_ratings=6, null_permutations=10,
                             max_vector_rows_in_result=10)
    targets = [
        {"handle": "h1", "tweet_ids": ["t1"], "tweets_scanned": 1},
        {"handle": "h2", "tweet_ids": ["t2"], "tweets_scanned": 1},
    ]
    notes = [
        {"noteId": "n1", "tweetId": "t1", "author_participant_id": "au", "createdAtMillis": 1,
         "classification": "MISINFORMED_OR_POTENTIALLY_MISLEADING", "summary_excerpt": "s1"},
        {"noteId": "n2", "tweetId": "t2", "author_participant_id": "au", "createdAtMillis": 2,
         "classification": "MISINFORMED_OR_POTENTIALLY_MISLEADING", "summary_excerpt": "s2"},
    ]
    ratings = {
        "n1": [{"participantId": f"r{i}", "createdAtMillis": i * 1000, "helpfulness": "HELPFUL"} for i in range(6)],
        "n2": [{"participantId": f"r{i}", "createdAtMillis": i * 1000, "helpfulness": "HELPFUL"} for i in range(6)],
    }
    data = {
        "ratings_by_note": ratings,
        "status_by_note": {},
        "total_ratings_kept": 12,
        "enrollment_rows": [{"participantId": f"r{i}", "createdAtMillis": 1_700_000_000_000} for i in range(6)],
        "baseline_by_rater": {f"r{i}": ["b1", "b2"] for i in range(6)},
        "baseline_rows_kept": 12,
        "enrollment_source_unavailable": False,
        "ratings_source": {"shard_urls": ["fake://0", "fake://1"], "shard_count": 2},
    }
    return p, targets, notes, data


def _fake_expansion(shard_urls, seed_ids, p):
    async def _fake(shard_urls, seed_ids, p):
        return {
            "seeds": sorted(seed_ids),
            "participants": {
                **{pid: {"level": 0, "shared": 4, "helpful": 4, "not_helpful": 0,
                         "match_helpful": 4, "match_not_helpful": 0} for pid in seed_ids},
                "newbie": {"level": 1, "shared": 2, "helpful": 2, "not_helpful": 0,
                           "match_helpful": 2, "match_not_helpful": 0},
            },
            "seed_notes": {"n1", "n2", "n3"},
            "seed_helpful_notes": 3,
            "seed_not_helpful_notes": 0,
            "levels_run": 1,
            "passes": [
                {"level": 1, "pass": "A", "shards": len(shard_urls), "rows_scanned": 100, "seed_notes": 3, "fanin_excluded": 0},
                {"level": 1, "pass": "B", "shards": len(shard_urls), "rows_scanned": 100, "raters_seen": 7},
            ],
        }
    return _fake(shard_urls, seed_ids, p)


def test_detect_wires_participant_expansion_after_other_vectors(monkeypatch):
    p, targets, notes, data = _detect_fixture()
    seen = {}

    async def fake(shard_urls, seed_ids, p):
        seen["urls"] = list(shard_urls)
        seen["seeds"] = list(seed_ids)
        return await _fake_expansion(shard_urls, seed_ids, p)

    monkeypatch.setattr(mod, "run_participant_expansion", fake)
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    vecs = f["vectors"]
    # the six pre-existing vectors are unchanged in shape, plus the new one
    assert set(vecs) == {
        "cross_handle_rater_map", "direction_lockstep", "enrollment_cohorts",
        "note_author_coordination", "synchronized_bursts", "baseline_null_model",
        "participant_expansion",
    }
    assert "_full_shared_raters" not in vecs["synchronized_bursts"]  # private key never emitted
    v7 = vecs["participant_expansion"]
    assert seen["urls"] == ["fake://0", "fake://1"]                # already-resolved shard URLs reused
    assert seen["seeds"] == [f"r{i}" for i in range(6)]             # seeds = V1 flagged raters (full set)
    s = v7["summary"]
    assert s["seeds"] == 6 and s["participants_total"] == 7
    assert s["participants_per_level"] == {"0": 6, "1": 1}
    assert s["seed_notes"] == 3 and s["levels_run"] == 1
    assert "newbie" in v7["participant_ids"]
    row = next(r for r in v7["rows"] if r["participantId"] == "newbie")
    assert row["expansion_level"] == 1 and row["shared_seed_notes"] == 2
    seed_row = next(r for r in v7["rows"] if r["participantId"] == "r0")
    assert seed_row["expansion_level"] == 0 and seed_row["in_v1"] is True and seed_row["in_lockstep"] is True
    assert seed_row["handles_covered"] == ["h1", "h2"]  # via the existing note_handle map
    assert seed_row["baseline_sample_size"] == 2
    assert v7["params"]["expand_min_shared_notes"] == 2 and v7["params"]["expand_seed_max"] == 5000
    assert len(v7["rows"]) <= p.max_vector_rows_in_result
    # per-handle findings unchanged in shape
    assert set(f["per_handle"]["h1"]) >= {"clusters", "bursts", "status_flips", "vector_hits",
                                          "coordination_score", "severity", "noted_tweet_ids"}


def test_detect_trims_participant_ids_to_fit_payload_cap(monkeypatch):
    import json
    p, targets, notes, data = _detect_fixture()
    p.expand_max_participants = 30000
    many = {f"{i:064x}": {"level": 1, "shared": 2, "helpful": 2, "not_helpful": 0,
                           "match_helpful": 2, "match_not_helpful": 0} for i in range(30000)}

    async def fake(shard_urls, seed_ids, p):
        return {
            "seeds": sorted(seed_ids),
            "participants": {**{pid: {"level": 0, "shared": 4, "helpful": 4, "not_helpful": 0,
                                       "match_helpful": 4, "match_not_helpful": 0} for pid in seed_ids},
                             **many},
            "seed_notes": {"n1", "n2", "n3"},
            "seed_helpful_notes": 3, "seed_not_helpful_notes": 0,
            "levels_run": 1, "passes": [],
        }

    monkeypatch.setattr(mod, "run_participant_expansion", fake)
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    v7 = f["vectors"]["participant_expansion"]
    assert len(json.dumps(f)) <= 1_800_000
    assert len(v7["participant_ids"]) < 30000
    assert v7["participant_ids_truncated_to"] == len(v7["participant_ids"])
    assert v7["summary"]["participants_total"] == 30006  # summary counts are not trimmed


def test_detect_marks_expansion_unavailable_when_ratings_source_unavailable():
    p, targets, notes, data = _detect_fixture()
    data.pop("ratings_source")
    data["ratings_source_unavailable"] = True
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    v7 = f["vectors"]["participant_expansion"]
    assert v7["unavailable"] is True
    assert v7["rows"] == [] and v7["participant_ids"] == []
    assert all(v is None for v in v7["summary"].values())  # null, not zero


def test_detect_omits_expansion_when_disabled_even_with_shard_urls():
    p, targets, notes, data = _detect_fixture()
    p.expand_participants = False
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    assert "participant_expansion" not in f["vectors"]
    p2, targets, notes, data = _detect_fixture()
    p2.vectors = {"participant_expansion": False}
    f2 = asyncio.run(detect_coordination(targets, notes, data, p2))
    assert "participant_expansion" not in f2["vectors"]
    assert len(f2["vectors"]) == 6


def test_detect_omits_expansion_without_shard_urls_and_not_unavailable():
    # unit-test shape (no ratings_source): the streaming vector is simply not emitted
    p, targets, notes, data = _detect_fixture()
    data.pop("ratings_source")
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    assert "participant_expansion" not in f["vectors"]
    assert len(f["vectors"]) == 6


# ── markdown rendering + LLM assessment augmentation ──


def test_render_markdown_renders_participant_expansion_vector():
    p, targets, notes, data = _detect_fixture()
    findings = {
        "per_handle": {"h1": {"tweets_scanned": 1, "notes_on_their_posts": 1, "noted_tweet_ids": ["t1"],
                              "clusters": [], "bursts": [], "status_flips": [], "vector_hits": {},
                              "coordination_score": 10.0, "severity": "low"}},
        "vectors": {
            "participant_expansion": {
                "params": {"expand_levels": 1},
                "summary": {"seeds": 6, "participants_total": 7, "participants_per_level": {"0": 6, "1": 1},
                            "tier_counts": {"high": 1, "medium": 2, "low": 4}, "tier_high": 1,
                            "tier_medium": 2, "tier_low": 4, "seed_notes": 3, "levels_run": 1,
                            "max_handles_covered": 2},
                "passes": [],
                "rows": [{"participantId": "abc", "expansion_level": 0, "score": 61.0, "tier": "high"}],
                "participant_ids": ["abc"],
            }
        },
    }
    md = _render_markdown(targets, findings, None, p)
    assert "### participant_expansion" in md
    assert "participants_total=7" in md
    assert '"participantId": "abc"' in md


def test_augment_assessment_adds_paragraph_entry_and_caveat():
    exp = {
        "unavailable": False,
        "summary": {"seeds": 6, "seed_notes": 3, "participants_total": 7,
                    "participants_per_level": {"0": 6, "1": 1},
                    "tier_counts": {"high": 1, "medium": 2, "low": 4},
                    "max_handles_covered": 2},
    }
    out = _augment_assessment_with_expansion(
        {"overall_summary": "Base summary.", "vectors": {}, "methodology_caveats": ["existing"]}, exp)
    assert out["overall_summary"].startswith("Base summary.")
    para = out["overall_summary"].split("\n\n")[1]
    assert "seeded 6" in para and "7 participantIds" in para and "level 0: 6" in para and "level 1: 1" in para
    assert "high=1" in para and "medium=2" in para and "low=4" in para
    assert "pseudonymous" in para
    assert out["vectors"]["participant_expansion"]["evidence"]
    assert any("organic raters of viral notes" in c and "deleted accounts" in c
               for c in out["methodology_caveats"])
    assert "existing" in out["methodology_caveats"]
    # unavailable variant: null paragraph, no caveat
    out2 = _augment_assessment_with_expansion(
        {"overall_summary": "x", "vectors": {}, "methodology_caveats": []},
        {"unavailable": True, "summary": {}})
    assert "unavailable" in out2["overall_summary"]
    assert out2["vectors"]["participant_expansion"]["assessment"].startswith("unavailable")
    assert out2["methodology_caveats"] == []


def test_pass_b_compact_storage_matches_dict_view():
    # pass B stores one pid->index dict + a flat int64 array (not one dict per rater) so
    # multi-million-rater fan-out cannot OOM the worker; iter_stats must match result()
    rows = [
        _row("n1", "r1", "HELPFUL"), _row("n1", "r2", "NOT_HELPFUL"),
        _row("n2", "r1", "NOT_HELPFUL"), _row("n2", "r1", "HELPFUL"),
        _row("n9", "r3", "HELPFUL"),  # not a seed note: ignored
    ]
    majority = {"n1": "helpful", "n2": "not_helpful"}
    pb = mod._ExpansionPassB({"n1", "n2"}, majority)
    for r in rows:
        pb.update(r)
    assert len(pb.idx) == 2 and len(pb.stats) == 2 * pb.SLOTS
    via_result = expansion_pass_b(rows, {"n1", "n2"}, majority)
    assert dict(pb.iter_stats()) == via_result["rater_stats"]
    assert via_result["rater_stats"]["r1"] == {"shared": 3, "helpful": 2, "not_helpful": 1,
                                               "match_helpful": 1, "match_not_helpful": 1}
    assert via_result["rater_stats"]["r2"] == {"shared": 1, "helpful": 0, "not_helpful": 1,
                                               "match_helpful": 0, "match_not_helpful": 0}


# ── vector builder memory bound (OOM regression) ──


def test_vector_builder_scores_all_but_materializes_only_capped_rows(monkeypatch):
    # pass B can surface ~1M+ raters on seed notes; the builder must score every participant
    # for the summary in one pass but only build full row dicts for the capped top rows/ids
    n = 5000
    participants = {
        f"p{i:06d}": {"level": 1, "shared": i % 50, "helpful": i % 50, "not_helpful": 0,
                      "match_helpful": i % 50, "match_not_helpful": 0}
        for i in range(n)
    }
    participants["s0"] = {"level": 0, "shared": 99, "helpful": 99, "not_helpful": 0,
                          "match_helpful": 99, "match_not_helpful": 0}
    expansion = {
        "seeds": ["s0"], "participants": participants, "seed_notes": {"n1"},
        "seed_helpful_notes": 1, "seed_not_helpful_notes": 0, "levels_run": 1, "passes": [],
    }
    p = AstroturfMapperInput(target_handles=["h1"], max_vector_rows_in_result=7, expand_max_participants=13)
    calls = []
    real = mod.score_participant

    def counting(*a, **kw):
        calls.append(a[0])
        return real(*a, **kw)

    monkeypatch.setattr(mod, "score_participant", counting)
    vec = build_participant_expansion_vector(expansion, {}, {}, {}, None, p)
    assert vec["summary"]["participants_total"] == n + 1          # summary covers everyone
    assert sum(vec["summary"]["tier_counts"].values()) == n + 1
    assert vec["summary"]["participants_per_level"] == {"0": 1, "1": n}
    assert len(vec["rows"]) == 7 and len(calls) == 7                # only capped rows materialized
    assert len(vec["participant_ids"]) == 13
    assert vec["rows"][0]["participantId"] == "s0"                 # highest score first
    assert vec["participant_ids"][0] == "s0"
    # ids are the top expand_max_participants by (-score, -shared, participantId)
    keys = sorted(
        ((-mod.participant_score(st["shared"], mod.same_direction_jaccard(st, 1, 0), 0,
                                 False, False, False, False, st["level"])[0], -st["shared"], pid), pid)
        for pid, st in participants.items()
    )
    assert vec["participant_ids"] == [pid for _, pid in keys[:13]]
    assert [r["participantId"] for r in vec["rows"]] == [pid for _, pid in keys[:7]]
