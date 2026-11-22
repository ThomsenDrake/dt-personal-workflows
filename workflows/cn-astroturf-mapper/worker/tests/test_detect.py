"""Unit tests for the pure coordination-detection helpers in cn_astroturf_mapper."""
import asyncio

from worker.workflows.cn_astroturf_mapper import (
    AstroturfMapperInput,
    _baseline_reservoir_update,
    _cap_raters,
    _choose_baseline_raters,
    _denormalize_data,
    _omit_tweet_ids_from_result,
    _permute_note_labels,
    _public_grok_auth,
    _rater_pid,
    baseline_vs_target_overlap,
    cross_handle_rater_map,
    detect_bursts,
    detect_coordination,
    detect_status_flips,
    direction_lockstep,
    find_corating_clusters,
    find_direction_clusters,
    find_enrollment_cohorts,
    note_author_coordination,
    permutation_null,
    permutation_p_value,
    score_handle,
    synchronized_bursts,
)


def test_clusters_group_high_overlap_raters():
    rater_notes = {
        "a": {"n1", "n2", "n3", "n4"},
        "b": {"n1", "n2", "n3", "n4"},
        "c": {"n1", "n2", "n3", "n5"},
        "d": {"n1", "n2", "n3", "n4"},
        "e": {"n1", "n2", "n3", "n4"},
        "loner": {"x1", "x2", "x3"},
    }
    clusters = find_corating_clusters(rater_notes, min_overlap=0.5, min_cluster_size=5, min_notes=3)
    assert len(clusters) == 1
    assert clusters[0]["size"] == 5
    assert "loner" not in clusters[0]["member_participant_ids"]


def test_clusters_empty_when_too_few_raters():
    rater_notes = {"a": {"n1", "n2"}, "b": {"n1", "n2"}}
    assert find_corating_clusters(rater_notes, min_overlap=0.5, min_cluster_size=5, min_notes=3) == []


def test_burst_detects_one_sided_window():
    ratings = [{"createdAtMillis": i * 1000, "helpfulness": "HELPFUL"} for i in range(10)]
    bursts = detect_bursts({"n1": ratings}, window_ms=30_000, min_ratings=8, one_sided_ratio=0.8)
    assert len(bursts) == 1
    assert bursts[0]["count"] == 10
    assert bursts[0]["direction"] == "helpful"
    assert bursts[0]["one_sided_ratio"] == 1.0


def test_burst_ignores_mixed_window():
    ratings = [
        {"createdAtMillis": i * 1000, "helpfulness": "HELPFUL" if i % 2 == 0 else "NOT_HELPFUL"}
        for i in range(10)
    ]
    bursts = detect_bursts({"n1": ratings}, window_ms=30_000, min_ratings=8, one_sided_ratio=0.8)
    assert bursts == []


def test_status_flip_detected_after_helpful():
    hist = [
        {"createdAtMillis": 1, "status": "CURRENTLY_RATED_HELPFUL"},
        {"createdAtMillis": 2, "status": "NEEDS_MORE_RATINGS"},
    ]
    flips = detect_status_flips({"n1": hist})
    assert len(flips) == 1
    assert flips[0]["new_status"] == "NEEDS_MORE_RATINGS"


def test_score_handle_scales_and_caps():
    assert score_handle(0, [], [], []) == (0.0, "none")
    big_cluster = {"size": 20}
    score, severity = score_handle(10, [big_cluster, dict(big_cluster)], [{"x": 1}] * 10, [{"y": 1}] * 10)
    assert score == 80.0
    assert severity == "critical"


def test_detect_reports_noted_tweet_ids():
    p = AstroturfMapperInput(target_handles=["h"])
    targets = [{"handle": "h", "tweet_ids": ["t1", "t2"], "tweets_scanned": 2}]
    notes = [{"noteId": "n1", "tweetId": "t1", "author_participant_id": "x", "createdAtMillis": 1,
              "classification": "MISINFORMED_OR_POTENTIALLY_MISLEADING", "summary_excerpt": "s"}]
    data = {"ratings_by_note": {}, "status_by_note": {}, "total_ratings_kept": 0}

    f = asyncio.run(detect_coordination(targets, notes, data, p))
    assert f["per_handle"]["h"]["noted_tweet_ids"] == ["t1"]
    assert f["per_handle"]["h"]["notes_on_their_posts"] == 1


# ── V1: cross_handle_rater_map ──


def test_v1_rater_spanning_4_handles_is_reported():
    rater_note_dir = {
        "r1": {"n1": "helpful", "n2": "not_helpful", "n3": "helpful", "n4": "helpful", "n5": "not_helpful"},
        "r2": {"n1": "helpful"},
    }
    note_handle = {"n1": "h1", "n2": "h2", "n3": "h3", "n4": "h4", "n5": "h1"}
    res = cross_handle_rater_map(rater_note_dir, note_handle, min_handles=2, max_rows=10)
    assert res["summary"]["raters_flagged"] == 1
    assert res["summary"]["handles_covered_distribution"] == {"1": 1, "4": 1}
    row = res["rows"][0]
    assert row["participantId"] == "r1"
    assert row["handles_covered"] == 4
    assert row["handles"] == ["h1", "h2", "h3", "h4"]
    assert row["per_handle"]["h1"] == {"notes": 2, "helpful": 1, "not_helpful": 1}
    assert row["per_handle"]["h2"] == {"notes": 1, "helpful": 0, "not_helpful": 1}


def test_v1_row_cap_respected():
    rater_note_dir = {f"r{i}": {"n1": "helpful", "n2": "helpful"} for i in range(10)}
    note_handle = {"n1": "h1", "n2": "h2"}
    res = cross_handle_rater_map(rater_note_dir, note_handle, min_handles=2, max_rows=3)
    assert len(res["rows"]) == 3
    assert res["summary"]["raters_flagged"] == 10


# ── V2: direction_lockstep + direction-aware clusters ──


def test_v2_one_sided_rater_flagged_per_handle_aggregate():
    rater_note_dir = {
        "lock": {f"n{i}": "helpful" for i in range(1, 6)},
        "mixed": {"n1": "helpful", "n2": "not_helpful", "n3": "helpful", "n4": "not_helpful", "n5": "helpful"},
    }
    note_handle = {f"n{i}": "h1" for i in range(1, 6)}
    res = direction_lockstep(rater_note_dir, note_handle, min_ratings=5, one_sided_threshold=0.9, max_rows=10)
    assert {r["participantId"] for r in res["rows"]} == {"lock"}
    assert res["rows"][0]["dominant_direction"] == "helpful"
    assert res["rows"][0]["one_sided_ratio"] == 1.0
    assert res["per_handle"]["h1"]["raters"] == 2
    assert res["per_handle"]["h1"]["flagged_raters"] == 1
    assert res["per_handle"]["h1"]["flagged_share"] == 0.5
    assert res["per_handle"]["h1"]["helpful_ratio"] == 0.8


def test_v2_direction_clusters_do_not_link_opposite_directions():
    same = {f"n{i}" for i in range(1, 6)}
    rater_note_dir = {
        "a": {n: "helpful" for n in same},
        "b": {n: "not_helpful" for n in same},  # same notes, OPPOSITE direction
        "c": {n: "helpful" for n in same},
        "d": {n: "helpful" for n in same},
    }
    clusters = find_direction_clusters(rater_note_dir, min_overlap=0.5, min_cluster_size=2, min_notes=3)
    assert len(clusters) == 1
    assert set(clusters[0]["member_participant_ids"]) == {"a", "c", "d"}
    assert "b" not in clusters[0]["member_participant_ids"]


# ── V3: enrollment cohorts ──


def test_v3_enrollment_cohort_detected_and_cross_referenced():
    day_ms = 1_700_000_000_000  # 2023-11-14 UTC
    rows = [{"participantId": f"p{i}", "createdAtMillis": day_ms} for i in range(12)]
    rows.append({"participantId": "other", "createdAtMillis": day_ms + 3 * 86_400_000})
    res = find_enrollment_cohorts(rows, min_cohort_size=10, max_rows=5,
                                  cross_handle_ids={"p0", "p1"}, lockstep_ids={"p2"})
    assert res["summary"]["cohorts_flagged"] == 1
    assert res["summary"]["creation_days_seen"] == 2
    row = res["rows"][0]
    assert row["cohort_size"] == 12
    assert row["creation_day_utc"] == "2023-11-14"
    assert row["members_in_cross_handle_map"] == 2
    assert row["members_flagged_lockstep"] == 1
    assert len(row["member_participant_ids"]) == 5  # capped at max_rows


def test_v3_small_day_not_flagged():
    rows = [{"participantId": f"p{i}", "createdAtMillis": 1_700_000_000_000} for i in range(3)]
    res = find_enrollment_cohorts(rows, min_cohort_size=10, max_rows=5)
    assert res["summary"]["cohorts_flagged"] == 0
    assert res["rows"] == []


# ── V4: note_author_coordination ──


def test_v4_author_spanning_handles_and_duplicate_summaries():
    notes = [
        {"noteId": "n1", "tweetId": "t1", "author_participant_id": "au",
         "summary_excerpt": "alpha beta gamma delta epsilon zeta eta"},
        {"noteId": "n2", "tweetId": "t2", "author_participant_id": "au",
         "summary_excerpt": "alpha beta gamma delta epsilon zeta eta"},
        {"noteId": "n3", "tweetId": "t3", "author_participant_id": "other",
         "summary_excerpt": "totally different words here now yes indeed"},
    ]
    note_handle = {"n1": "h1", "n2": "h2", "n3": "h1"}
    res = note_author_coordination(notes, note_handle, max_rows=10)
    assert res["summary"]["authors_spanning_handles"] == 1
    assert res["multi_handle_authors"][0]["participantId"] == "au"
    assert res["multi_handle_authors"][0]["handles"] == ["h1", "h2"]
    assert res["summary"]["near_duplicate_summary_pairs"] == 1
    assert res["duplicate_summary_pairs"][0]["shingle_jaccard"] == 1.0
    assert res["duplicate_summary_pairs"][0]["handles"] == ["h1", "h2"]


# ── V5: synchronized cross-note bursts ──


def _burst_note(raters, base):
    return [{"participantId": r, "createdAtMillis": base + i * 1000, "helpfulness": "NOT_HELPFUL"}
            for i, r in enumerate(raters)]


def test_v5_synchronized_burst_flagged():
    raters = [f"r{i}" for i in range(6)]
    ratings = {"n1": _burst_note(raters, 1_000_000), "n2": _burst_note(raters, 1_000_000 + 5_000)}
    note_handle = {"n1": "h1", "n2": "h2"}
    res = synchronized_bursts(ratings, note_handle, window_ms=30_000, min_ratings=6,
                              one_sided_ratio=0.8, min_shared_raters=5, max_rows=10)
    assert res["summary"]["sync_groups_flagged"] == 1
    row = res["rows"][0]
    assert row["shared_rater_count"] == 6
    assert set(row["notes"]) == {"n1", "n2"}
    assert set(row["handles"]) == {"h1", "h2"}


def test_v5_distant_windows_not_synchronized():
    raters = [f"r{i}" for i in range(6)]
    ratings = {"n1": _burst_note(raters, 1_000_000), "n2": _burst_note(raters, 1_000_000 + 10 * 60_000)}
    note_handle = {"n1": "h1", "n2": "h2"}
    res = synchronized_bursts(ratings, note_handle, window_ms=30_000, min_ratings=6,
                              one_sided_ratio=0.8, min_shared_raters=5, max_rows=10)
    assert res["summary"]["sync_groups_flagged"] == 0


# ── V6: baseline / permutation null ──


def test_v6_permutation_null_p_value_in_unit_interval():
    same = {"n1", "n2", "n3", "n4"}
    rater_notes = {f"r{i}": set(same) for i in range(6)}
    rater_notes["loner"] = {"x1", "x2", "x3"}
    null = permutation_null(rater_notes, min_overlap=0.5, min_cluster_size=5, min_notes=3, permutations=20)
    p = null["summary"]["p_value_max_cluster_size"]
    assert 0.0 <= p <= 1.0
    assert null["summary"]["observed_max_cluster_size"] == 6
    assert len(null["null_max_cluster_size"]) == 20
    assert 0.0 <= null["summary"]["p_value_mean_overlap"] <= 1.0
    assert permutation_p_value(5, [5, 5], 10) == round(3 / 11, 4)
    assert permutation_p_value(99, [1, 2], 10) == round(1 / 11, 4)


def test_v6_baseline_vs_target_overlap():
    rater_notes = {"a": {"n1", "n2"}, "b": {"n1", "n2"}, "c": {"n1", "n2"}}
    baseline = {"a": ["b1", "b2"], "b": ["b1", "b2"], "c": ["b3"]}
    clusters = [{"cluster_id": "C-x", "size": 3, "member_participant_ids": ["a", "b", "c"]}]
    rows = baseline_vs_target_overlap(rater_notes, baseline, clusters)
    assert rows[0]["mean_target_overlap"] == 1.0
    assert rows[0]["mean_baseline_overlap"] == round(1.0 / 3, 4)
    assert rows[0]["overlap_lift"] == round(1.0 - 1.0 / 3, 4)


# ── detect_coordination wiring ──


def _two_handle_fixture():
    p = AstroturfMapperInput(target_handles=["h1", "h2"], min_notes_for_rater=2, min_cluster_size=5,
                             direction_min_ratings=2, burst_min_ratings=6, null_permutations=10)
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
    }
    return p, targets, notes, data


def test_detect_coordination_emits_all_vectors():
    p, targets, notes, data = _two_handle_fixture()
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    vecs = f["vectors"]
    assert set(vecs) == {
        "cross_handle_rater_map", "direction_lockstep", "enrollment_cohorts",
        "note_author_coordination", "synchronized_bursts", "baseline_null_model",
    }
    v1 = vecs["cross_handle_rater_map"]
    assert v1["summary"]["raters_flagged"] == 6  # every rater rated notes on both handles
    assert all(r["handles_covered"] == 2 for r in v1["rows"])
    assert vecs["direction_lockstep"]["summary"]["flagged_raters"] == 6
    assert vecs["direction_lockstep"]["summary"]["direction_clusters_flagged"] >= 1
    assert vecs["note_author_coordination"]["summary"]["authors_spanning_handles"] == 1
    assert vecs["synchronized_bursts"]["summary"]["sync_groups_flagged"] == 1
    assert 0.0 <= vecs["baseline_null_model"]["summary"]["p_value_max_cluster_size"] <= 1.0
    assert f["all_clusters"] and f["all_clusters"][0].get("permutation_p_value") is not None
    hits = f["per_handle"]["h1"]["vector_hits"]
    assert hits["cross_handle_raters"] == 6
    assert hits["lockstep_raters"] == 6
    # existing fields untouched
    assert f["per_handle"]["h1"]["noted_tweet_ids"] == ["t1"]
    assert set(f) >= {"per_handle", "all_clusters", "cluster_note_ids", "bursts", "flips", "notes", "vectors"}


def test_detect_coordination_respects_vector_flags_and_row_caps():
    p, targets, notes, data = _two_handle_fixture()
    p.vectors = {"baseline_null_model": False, "enrollment_cohorts": False}
    p.max_vector_rows_in_result = 2
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    assert "baseline_null_model" not in f["vectors"]
    assert "enrollment_cohorts" not in f["vectors"]
    assert len(f["vectors"]["cross_handle_rater_map"]["rows"]) == 2
    assert f["vectors"]["cross_handle_rater_map"]["summary"]["raters_flagged"] == 6
    assert f["all_clusters"] == [] or "permutation_p_value" not in f["all_clusters"][0]


def test_detect_coordination_enrollment_unavailable_is_null_not_zero():
    p, targets, notes, data = _two_handle_fixture()
    data["enrollment_source_unavailable"] = True
    data["enrollment_rows"] = []
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    v3 = f["vectors"]["enrollment_cohorts"]
    assert v3["unavailable"] is True
    assert v3["rows"] == []
    assert all(v is None for v in v3["summary"].values())


def test_score_handle_vector_bonus_small_and_capped():
    base_score, base_sev = score_handle(10, [], [], [])
    boosted, _ = score_handle(10, [], [], [], {
        "cross_handle_raters": 100, "lockstep_raters": 100, "sync_burst_groups": 100,
        "author_coordination": 100, "enrollment_cohort_raters": 1000,
    })
    assert boosted == round(min(base_score + 10.0, 100.0), 1)
    assert score_handle(0, [], [], []) == (0.0, "none")  # unchanged legacy behavior


# ── compact cross-activity encoding + live column names ──


def test_rater_pid_column_probe():
    assert _rater_pid({"raterParticipantId": "abc"}) == "abc"  # current noteRatings header
    assert _rater_pid({"participantId": "def"}) == "def"        # older snapshots
    assert _rater_pid({}) is None


def test_denormalize_compact_ratings_round_trip():
    data = {
        "participants": ["r0", "r1"],
        "ratings_by_note": {"n1": [[0, 100, "H"], [1, 200, "N"], [0, 300, ""]]},
        "status_by_note": {"n1": [[5, "CURRENTLY_RATED_HELPFUL"]]},
        "baseline_by_rater": {"0": ["b1", "b2"]},
    }
    out = _denormalize_data(data)
    assert out["ratings_by_note"]["n1"][0] == {"noteId": "n1", "participantId": "r0", "createdAtMillis": 100, "helpfulness": "HELPFUL"}
    assert out["ratings_by_note"]["n1"][1]["helpfulness"] == "NOT_HELPFUL"
    assert out["ratings_by_note"]["n1"][2]["helpfulness"] == ""
    assert out["status_by_note"]["n1"][0] == {"noteId": "n1", "createdAtMillis": 5, "status": "CURRENTLY_RATED_HELPFUL"}
    assert out["baseline_by_rater"] == {"r0": ["b1", "b2"]}


def test_detect_coordination_accepts_compact_load_shape():
    p, targets, notes, data = _two_handle_fixture()
    compact = {
        "participants": [f"r{i}" for i in range(6)],
        "ratings_by_note": {
            "n1": [[i, i * 1000, "H"] for i in range(6)],
            "n2": [[i, i * 1000, "H"] for i in range(6)],
        },
        "status_by_note": {},
        "baseline_by_rater": {str(i): ["b1"] for i in range(6)},
        "total_ratings_kept": 12,
        "enrollment_ms": [1_700_000_000_000] * 6,  # index-aligned ts list (payload cap)
        "enrollment_source_unavailable": False,
    }
    f = asyncio.run(detect_coordination(targets, notes, compact, p))
    v1 = f["vectors"]["cross_handle_rater_map"]
    assert v1["summary"]["raters_flagged"] == 6
    assert v1["rows"][0]["participantId"] == "r0"
    assert f["all_clusters"][0]["member_participant_ids"] == [f"r{i}" for i in range(6)]
    # enrollment_ms was expanded back into rows (all 6 on the same UTC day; cohort < 10 not flagged)
    assert f["vectors"]["enrollment_cohorts"]["summary"]["raters_with_enrollment"] == 6
    assert f["vectors"]["enrollment_cohorts"]["summary"]["cohorts_flagged"] == 0


# ── Review-fix regression tests (PR #17 morning-star findings 1-8) ──

import random as _random


def test_f1_missing_baseline_is_null_not_zero():
    # cluster members with NO baseline sample must report None, not a fabricated 0.0/lift
    rater_notes = {"a": {"n1", "n2"}, "b": {"n1", "n2"}, "c": {"n1", "n2"}}
    clusters = [{"cluster_id": "C-x", "size": 3, "member_participant_ids": ["a", "b", "c"]}]
    rows = baseline_vs_target_overlap(rater_notes, {}, clusters)
    assert rows[0]["mean_target_overlap"] == 1.0
    assert rows[0]["mean_baseline_overlap"] is None
    assert rows[0]["overlap_lift"] is None
    # partial baseline: only members WITH a baseline sample contribute to the mean
    rows = baseline_vs_target_overlap(rater_notes, {"a": ["b1"], "b": ["b1"]}, clusters)
    assert rows[0]["mean_baseline_overlap"] == 1.0  # only the (a,b) pair has baseline data
    assert rows[0]["overlap_lift"] == 0.0


def test_f2_permutation_null_preserves_set_sizes():
    rng = _random.Random(7)
    sub = {"r1": {"n1", "n2", "n3"}, "r2": {"n1", "n2"}, "r3": {"n4"}}
    pool = sorted({"n1", "n2", "n3", "n4", "n5"})
    for _ in range(50):
        perm = _permute_note_labels(sub, pool, rng)
        for r, notes in sub.items():
            assert len(perm[r]) == len(notes)  # exact size preserved (no with-replacement shrinkage)
            assert perm[r] <= set(pool)
    # end-to-end: p-values stay in [0, 1]
    null = permutation_null({"a": {"n1", "n2", "n3"}, "b": {"n1", "n2", "n3"}, "c": {"n1", "n2", "n3"}},
                            min_overlap=0.5, min_cluster_size=3, min_notes=3, permutations=20)
    assert 0.0 <= null["summary"]["p_value_max_cluster_size"] <= 1.0
    assert 0.0 <= null["summary"]["p_value_mean_overlap"] <= 1.0


def test_f3_negative_rater_index_maps_to_none_and_is_not_attributed():
    data = {
        "participants": ["r0"],
        "ratings_by_note": {"n1": [[0, 100, "H"], [-1, 200, "H"]]},  # -1 = missing rater column
        "status_by_note": {},
    }
    out = _denormalize_data(data)
    assert out["ratings_by_note"]["n1"][0]["participantId"] == "r0"
    assert out["ratings_by_note"]["n1"][1]["participantId"] is None  # NOT participants[-1]
    # and detect_coordination never builds a rater entry for the unknown rater
    p = AstroturfMapperInput(target_handles=["h"], max_vector_rows_in_result=10)
    targets = [{"handle": "h", "tweet_ids": ["t1"], "tweets_scanned": 1}]
    notes = [{"noteId": "n1", "tweetId": "t1", "author_participant_id": "x", "createdAtMillis": 1,
              "classification": "MISINFORMED_OR_POTENTIALLY_MISLEADING", "summary_excerpt": "s"}]
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    v1 = f["vectors"]["cross_handle_rater_map"]
    assert v1["summary"]["raters_total"] == 1  # only r0; None is not a rater
    assert all(r["participantId"] != None for r in v1["rows"]) if v1["rows"] else True


def test_f4_v3_cross_reference_uses_full_uncapped_sets():
    p, targets, notes, data = _two_handle_fixture()
    p.max_vector_rows_in_result = 2          # emitted rows capped at 2 ...
    p.enrollment_cohort_min_size = 3         # ... but all 6 raters share one creation day
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    v2 = f["vectors"]["direction_lockstep"]
    assert len(v2["rows"]) == 2 and v2["summary"]["flagged_raters"] == 6
    assert len(v2["flagged_participant_ids"]) == 2  # emitted id list stays capped
    v3 = f["vectors"]["enrollment_cohorts"]
    row = v3["rows"][0]
    assert row["cohort_size"] == 6
    # ... while the cross-reference counts see the FULL flagged sets, not the top-2 rows
    assert row["members_in_cross_handle_map"] == 6
    assert row["members_flagged_lockstep"] == 6
    # per-handle score bonus is also exact (not derived from capped member lists)
    assert f["per_handle"]["h1"]["vector_hits"]["enrollment_cohort_raters"] == 6
    assert f["per_handle"]["h2"]["vector_hits"]["enrollment_cohort_raters"] == 6


def test_f5_direction_clusters_carry_p_value_in_output():
    p, targets, notes, data = _two_handle_fixture()
    f = asyncio.run(detect_coordination(targets, notes, data, p))
    dcs = f["vectors"]["direction_lockstep"]["direction_clusters"]
    assert dcs, "fixture should produce a direction cluster"
    for c in dcs:
        assert "permutation_p_value" in c  # attached to the EMITTED copies, not just the originals
        assert 0.0 <= c["permutation_p_value"] <= 1.0


def test_f6_public_grok_auth_strips_device_code():
    start = {"status": "pending", "verification_url": "https://x.ai/activate", "user_code": "ABCD-1234", "pid": 42}
    pub = _public_grok_auth({**start, "status": "authenticated"})
    assert "user_code" not in pub and "verification_url" not in pub
    assert pub["status"] == "authenticated" and pub["pid"] == 42
    assert _public_grok_auth(None) is None


def test_f7_omit_tweet_ids_also_strips_findings_notes():
    targets = [{"handle": "h", "tweet_ids": ["t1", "t2"], "tweets_scanned": 2}]
    findings = {
        "per_handle": {"h": {"noted_tweet_ids": ["t1"], "coordination_score": 1.0}},
        "notes": [{"noteId": "n1", "tweetId": "t1", "author_participant_id": "x"}],
    }
    _omit_tweet_ids_from_result(targets, findings)
    assert targets[0]["tweet_ids"] == []
    assert findings["per_handle"]["h"]["noted_tweet_ids"] == []
    assert findings["per_handle"]["h"]["noted_tweet_ids_count"] == 1
    assert "tweetId" not in findings["notes"][0]  # no tweet ids left anywhere in the payload
    assert findings["notes"][0]["noteId"] == "n1"


def test_f8_baseline_reservoir_is_order_independent_and_bounded():
    # pass-2 reservoir: a rater's non-target rows are sampled uniformly regardless of the
    # order they arrive in (they may all precede the rater's first target-note row)
    rng = _random.Random(3)
    res, seen = {}, {}
    for i in range(100):  # 100 non-target rows for rater 0, arriving "before" any target row
        _baseline_reservoir_update(res, seen, 0, f"b{i}", cap=20, rng=rng)
    assert len(res[0]) == 20 and len(set(res[0])) == 20  # per-rater cap respected
    assert seen[0] == 100
    _baseline_reservoir_update(res, seen, 1, "x1", cap=20, rng=rng)
    assert res[1] == ["x1"]
    # rater-level cap keeps the map bounded (seeded sample)
    big = {i: [f"n{i}"] for i in range(1000)}
    capped = _cap_raters(big, 400, _random.Random(3))
    assert len(capped) == 400 and set(capped) <= set(big)
    assert _cap_raters(big, 2000, _random.Random(3)) is big  # under the cap: unchanged


def test_f9_unknown_rater_rows_do_not_crash_or_get_attributed_in_v5():
    # a ratings row with participantId None (missing rater column) must still count toward
    # window size/ratio but never enter the rater sets (sorted() would crash on None vs str)
    base = 1_700_000_000_000
    ratings = {
        "n1": [{"participantId": f"r{i}", "createdAtMillis": base + i * 1000, "helpfulness": "HELPFUL"} for i in range(6)]
              + [{"participantId": None, "createdAtMillis": base + 6000, "helpfulness": "HELPFUL"}],
        "n2": [{"participantId": f"r{i}", "createdAtMillis": base + i * 1000, "helpfulness": "HELPFUL"} for i in range(6)]
              + [{"participantId": None, "createdAtMillis": base + 6000, "helpfulness": "HELPFUL"}],
    }
    note_handle = {"n1": "h1", "n2": "h2"}
    out = synchronized_bursts(ratings, note_handle, window_ms=30_000, min_ratings=6,
                              one_sided_ratio=0.8, min_shared_raters=5, max_rows=10)
    assert out["summary"]["sync_groups_flagged"] == 1  # no TypeError
    row = out["rows"][0]
    assert row["shared_rater_count"] == 6
    assert None not in row["shared_raters"] and all(isinstance(p, str) for p in row["shared_raters"])


def test_f11_baseline_raters_are_pre_bounded_before_pass_2():
    # the rater set for the V6 pass-2 reservoirs must be chosen BEFORE the scan so peak
    # memory is bounded by max_raters * per-rater cap, not by #target-note raters
    raters = {f"r{i}" for i in range(100_000)}
    chosen = _choose_baseline_raters(raters, 400, _random.Random(11))
    assert len(chosen) == 400 and chosen <= raters
    assert chosen == _choose_baseline_raters(raters, 400, _random.Random(11))  # deterministic seed
    small = {"a", "b"}
    assert _choose_baseline_raters(small, 400, _random.Random(11)) == small  # under cap: keep all
    # _cap_raters stays as a no-op safeguard once the set is pre-bounded
    res = {i: ["n"] for i in range(400)}
    assert _cap_raters(res, 400, _random.Random(11)) is res
