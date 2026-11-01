from lucida._flags import FlagOptions, remote_body


def test_remote_body_carries_anon_distinct_id_only_when_set() -> None:
    b = remote_body("lc_pk", "u1", FlagOptions(anon_distinct_id="anon-9"), None)
    assert b["anon_distinct_id"] == "anon-9"
    assert "anon_distinct_id" not in remote_body("lc_pk", "u1", FlagOptions(), None)
