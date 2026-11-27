import json

from worker.workflows import cast_logic as cl


def test_chunk_text_short_passthrough():
    assert cl.chunk_text("hello world", 3000) == ["hello world"]
    assert cl.chunk_text("   ", 3000) == []


def test_chunk_text_splits_on_sentences():
    text = "One two. Three four! Five six? " + "Seven eight. " * 200
    chunks = cl.chunk_text(text, 100)
    assert all(len(c) <= 100 for c in chunks)
    assert " ".join(chunks).replace("  ", " ") == text.strip()


def test_chunk_text_hard_splits_long_sentence():
    text = "x" * 250
    chunks = cl.chunk_text(text, 100)
    assert [len(c) for c in chunks] == [100, 100, 50]


def test_batch_chapters_covers_every_chapter():
    chapters = [{"title": f"c{i}", "text": "a" * 60} for i in range(10)]
    batches = cl.batch_chapters(chapters, batch_chars=200)
    covered = sorted(i for b in batches for i in b["indices"])
    assert covered == list(range(10))
    assert all(len(b["text"]) <= 200 + 60 for b in batches)


def test_assign_voices_distinct_and_gender_matched():
    chars = [
        {"name": "Alice", "gender": "female", "mentions": 50, "description": "lead"},
        {"name": "Bob", "gender": "male", "mentions": 40, "description": "friend"},
        {"name": "Cleo", "gender": "female", "mentions": 5, "description": "minor"},
    ]
    cast, warnings = cl.assign_voices(chars, narrator_voice="voice_478", max_characters=8)
    assert warnings == []
    assert cast[0]["name"] == cl.NARRATOR and cast[0]["voice_id"] == "voice_478"
    voice_ids = [c["voice_id"] for c in cast]
    assert len(set(voice_ids)) == len(voice_ids)
    by_name = {c["name"]: c for c in cast}
    assert by_name["Alice"]["voice_id"] != "voice_478"
    catalog = {v["voice_id"]: v for v in cl.VOICE_CATALOG}
    assert catalog[by_name["Alice"]["voice_id"]]["gender"] == "female"
    assert catalog[by_name["Bob"]["voice_id"]]["gender"] == "male"


def test_assign_voices_cap_folds_overflow_into_narrator():
    chars = [{"name": f"C{i}", "gender": "", "mentions": 100 - i} for i in range(12)]
    cast, warnings = cl.assign_voices(chars, max_characters=8)
    voiced = [c for c in cast if not c["is_narrator"]]
    assert len(voiced) == 8
    assert voiced[0]["name"] == "C0"
    assert any("fold into the narrator" in w for w in warnings)


def test_assign_voices_overrides_win_and_unknown_warns():
    chars = [{"name": "Alice", "gender": "female", "mentions": 10}]
    cast, warnings = cl.assign_voices(chars, voice_overrides={"alice": "voice_591", "Ghost": "voice_485"})
    by_name = {c["name"]: c for c in cast}
    assert by_name["Alice"]["voice_id"] == "voice_591"
    assert any("Ghost" in w for w in warnings)


def test_parse_json_object_tolerates_fences_and_prose():
    assert cl.parse_json_object('```json\n{"a": 1}\n```') == {"a": 1}
    assert cl.parse_json_object('Here you go: {"b": [2, 3]} hope that helps') == {"b": [2, 3]}
    try:
        cl.parse_json_object("no json at all")
        assert False
    except ValueError:
        pass


def test_prompts_mention_full_book_scan_and_cast_names():
    cast = [{"name": "Narrator"}, {"name": "Alice"}, {"name": "Bob"}]
    seg = cl.segment_prompt("Ch 1", "text", json.dumps(cast))
    assert "Alice" in seg and "Bob" in seg and "Narrator" in seg
    scan = cl.character_scan_prompt("some text")
    assert "JSON" in scan and "some text" in scan


def test_render_script_markdown():
    md = cl.render_script_markdown(
        "Book",
        [{"name": "Narrator", "voice_id": "voice_478", "voice_name": "Sheila", "is_narrator": True, "description": ""},
         {"name": "Alice", "voice_id": "voice_447", "voice_name": "Margaret", "is_narrator": False, "description": "lead"}],
        [{"title": "One", "segments": [{"speaker": "Narrator", "text": "It began."}, {"speaker": "Alice", "text": "Hi!"}]}],
    )
    assert "# Cast script: Book" in md
    assert "**Alice:** Hi!" in md
    assert "voice_447" in md
