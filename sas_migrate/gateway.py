"""All LLM traffic flows through BaseGateway. RestGatewayClient is the ONLY
module that will know the company gateway's REST contract; its _build_request
and _parse_response are filled in in-tenant."""
from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

from sas_migrate.config import MigrationConfig


class GatewayError(Exception):
    """Transport or protocol failure talking to the gateway."""


class CircuitOpenError(GatewayError):
    """Too many consecutive failures; halting to protect the shared gateway."""


class TokenBudgetExceeded(GatewayError):
    """A token cap was hit; caller must route the program to triage."""


@dataclass
class GatewayResponse:
    text: str
    input_tokens: int
    output_tokens: int


class TokenBudget:
    def __init__(self, cap: int):
        self.cap = cap
        self.used = 0

    def charge(self, n: int) -> None:
        self.used += n
        if self.used > self.cap:
            raise TokenBudgetExceeded(f"token budget exceeded: {self.used}/{self.cap}")


class BaseGateway:
    def __init__(self, on_call: Callable[[dict], None] | None = None):
        self._on_call = on_call

    def complete(self, system: str, messages: list[dict], *, model: str | None = None,
                 max_tokens: int | None = None, purpose: str = "",
                 program_id: str = "") -> GatewayResponse:
        raise NotImplementedError

    def _log(self, *, model: str, purpose: str, program_id: str,
             resp: GatewayResponse, latency_s: float) -> None:
        if self._on_call:
            self._on_call({
                "ts": time.time(), "model": model, "purpose": purpose,
                "program_id": program_id, "input_tokens": resp.input_tokens,
                "output_tokens": resp.output_tokens, "latency_s": round(latency_s, 3),
            })


class MockGateway(BaseGateway):
    """Scripted gateway for dev and tests. Raises if the script runs out."""

    def __init__(self, responses: list[str], on_call: Callable[[dict], None] | None = None):
        super().__init__(on_call)
        self._responses = list(responses)
        self.calls: list[dict] = []

    def complete(self, system, messages, *, model=None, max_tokens=None,
                 purpose="", program_id=""):
        if not self._responses:
            raise GatewayError("MockGateway script exhausted")
        self.calls.append({"system": system, "messages": messages, "model": model,
                           "purpose": purpose, "program_id": program_id})
        text = self._responses.pop(0)
        resp = GatewayResponse(text, input_tokens=len(str(messages)) // 4,
                               output_tokens=max(1, len(text) // 4))
        self._log(model=model or "mock", purpose=purpose, program_id=program_id,
                  resp=resp, latency_s=0.0)
        return resp


class RestGatewayClient(BaseGateway):
    def __init__(self, config: MigrationConfig, auth_token: str,
                 transport: Callable[[dict], dict] | None = None,
                 on_call: Callable[[dict], None] | None = None,
                 retry_sleep: Callable[[float], None] = time.sleep):
        super().__init__(on_call)
        self.config = config
        self.auth_token = auth_token
        self._transport = transport or self._http_post
        self._retry_sleep = retry_sleep
        self._consecutive_failures = 0

    # ------------- IN-TENANT FILL-IN POINTS -------------
    def _build_request(self, *, system: str, messages: list[dict], model: str,
                       max_tokens: int) -> dict:
        raise NotImplementedError(
            "Fill in with the company AI gateway request contract (in-tenant).")

    def _parse_response(self, raw: dict) -> GatewayResponse:
        raise NotImplementedError(
            "Fill in with the company AI gateway response contract (in-tenant).")
    # ----------------------------------------------------

    def _http_post(self, payload: dict) -> dict:
        req = urllib.request.Request(
            self.config.gateway_base_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {self.auth_token}"},
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=300) as f:
                return json.loads(f.read().decode())
        except Exception as e:  # noqa: BLE001 - normalize all transport errors
            raise GatewayError(f"gateway transport error: {e}") from e

    def complete(self, system, messages, *, model=None, max_tokens=None,
                 purpose="", program_id=""):
        threshold = self.config.gateway_circuit_breaker_threshold
        if self._consecutive_failures >= threshold:
            raise CircuitOpenError(
                f"{self._consecutive_failures} consecutive gateway failures; halting")
        model = model or self.config.default_model
        max_tokens = max_tokens or self.config.max_tokens_per_call
        payload = self._build_request(system=system, messages=messages,
                                      model=model, max_tokens=max_tokens)
        last_err: Exception | None = None
        for attempt in range(self.config.gateway_max_retries):
            start = time.time()
            try:
                raw = self._transport(payload)
                resp = self._parse_response(raw)
                self._consecutive_failures = 0
                self._log(model=model, purpose=purpose, program_id=program_id,
                          resp=resp, latency_s=time.time() - start)
                return resp
            except GatewayError as e:
                last_err = e
                self._retry_sleep(2 ** attempt)
        self._consecutive_failures += 1
        raise GatewayError(f"gateway failed after retries: {last_err}")
