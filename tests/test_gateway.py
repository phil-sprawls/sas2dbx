import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import (
    CircuitOpenError, GatewayError, GatewayResponse, MockGateway,
    RestGatewayClient, TokenBudget, TokenBudgetExceeded,
)


def test_mock_gateway_returns_scripted_responses_and_records_calls():
    gw = MockGateway(["first", "second"])
    r1 = gw.complete("sys", [{"role": "user", "content": "a"}], purpose="translate")
    r2 = gw.complete("sys", [{"role": "user", "content": "b"}])
    assert (r1.text, r2.text) == ("first", "second")
    assert gw.calls[0]["purpose"] == "translate"
    assert len(gw.calls) == 2


def test_token_budget_raises_when_exceeded():
    b = TokenBudget(100)
    b.charge(60)
    with pytest.raises(TokenBudgetExceeded):
        b.charge(50)
    assert b.used == 110  # charge recorded even when it trips


def test_rest_client_retries_transient_failures_then_succeeds():
    attempts = []

    def flaky_transport(payload):
        attempts.append(payload)
        if len(attempts) < 3:
            raise GatewayError("transient 503")
        return {"text": "ok", "input_tokens": 10, "output_tokens": 5}

    cfg = MigrationConfig(gateway_max_retries=4)
    client = RestGatewayClient(cfg, auth_token="t", transport=flaky_transport,
                               retry_sleep=lambda s: None)
    client._build_request = lambda **kw: {"payload": True}
    client._parse_response = lambda raw: GatewayResponse(raw["text"], raw["input_tokens"], raw["output_tokens"])
    resp = client.complete("sys", [{"role": "user", "content": "hi"}])
    assert resp.text == "ok"
    assert len(attempts) == 3


def test_circuit_breaker_opens_after_consecutive_failures():
    def dead_transport(payload):
        raise GatewayError("down")

    cfg = MigrationConfig(gateway_max_retries=1, gateway_circuit_breaker_threshold=2)
    client = RestGatewayClient(cfg, auth_token="t", transport=dead_transport,
                               retry_sleep=lambda s: None)
    client._build_request = lambda **kw: {}
    client._parse_response = lambda raw: None
    for _ in range(2):
        with pytest.raises(GatewayError):
            client.complete("sys", [{"role": "user", "content": "x"}])
    with pytest.raises(CircuitOpenError):
        client.complete("sys", [{"role": "user", "content": "x"}])


def test_on_call_logging_hook_receives_record():
    records = []
    gw = MockGateway(["hello"], on_call=records.append)
    gw.complete("sys", [{"role": "user", "content": "x"}], purpose="repair", program_id="p1")
    assert records[0]["purpose"] == "repair"
    assert records[0]["program_id"] == "p1"
    assert records[0]["output_tokens"] > 0


def test_rest_client_build_request_is_in_tenant_fill_in():
    client = RestGatewayClient(MigrationConfig(), auth_token="t")
    with pytest.raises(NotImplementedError):
        client.complete("sys", [{"role": "user", "content": "x"}])


def test_on_call_hook_receives_failure_record():
    records = []

    def dead_transport(payload):
        raise GatewayError("down")

    cfg = MigrationConfig(gateway_max_retries=2)
    client = RestGatewayClient(cfg, auth_token="t", transport=dead_transport,
                               on_call=records.append, retry_sleep=lambda s: None)
    client._build_request = lambda **kw: {}
    client._parse_response = lambda raw: None
    with pytest.raises(GatewayError):
        client.complete("sys", [{"role": "user", "content": "x"}])
    assert len(records) == 1
    assert "error" in records[0]
    assert records[0]["output_tokens"] == 0
