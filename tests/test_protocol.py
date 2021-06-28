from pg_purepy.protocol import ProtocolMachine, NEED_DATA, AuthenticationCompleted, ParameterStatus


def test_tricky_packet_truncation():
    """
    Tests when packets are truncated.
    """
    state = ProtocolMachine(username="postgres")
    state.do_startup()

    # This has one missing NUL from the end.
    state.receive_bytes(b"\x52\x00\x00\x00\x08\x00\x00\x00")
    assert state.next_event() == NEED_DATA

    # The null byte is prepended here.
    state.receive_bytes(
        b"\x00\x53\x00\x00\x00\x1f\x61\x70\x70\x6c\x69\x63\x61\x74\x69\x6f\x6e\x5f"
        b"\x6e"
        b"\x61\x6d\x65\x00\x70\x67\x2d\x70\x75\x72\x65\x70\x79\x00"
    )

    assert isinstance(state.next_event(), AuthenticationCompleted)
    status = state.next_event()
    assert isinstance(status, ParameterStatus)
    assert status.name == "application_name"
    assert status.value == "pg-purepy"
