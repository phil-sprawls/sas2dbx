"""Program inventory: registration, status tracking, resumability.
A killed batch run resumes by processing Inventory.pending()."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from sas_migrate.statestore import StateStore

STATUSES = ("registered", "landed", "translated", "validating",
            "parity_pass", "triage")
TABLE = "inventory"


@dataclass
class ProgramRecord:
    program_id: str
    sas_path: str
    owner: str
    inputs: dict = field(default_factory=dict)        # sas libref.table -> catalog table
    ground_truth: dict = field(default_factory=dict)  # sas output name -> catalog table
    status: str = "registered"
    error: str | None = None
    failure_mode: str | None = None   # never_ran | diverged | budget | None
    float_rel_tol: float | None = None  # per-program override; None = config default


class Inventory:
    def __init__(self, store: StateStore):
        self.store = store

    def register(self, rec: ProgramRecord) -> bool:
        if self.store.get(TABLE, rec.program_id) is not None:
            return False
        self.store.upsert(TABLE, rec.program_id, asdict(rec))
        return True

    def get(self, program_id: str) -> ProgramRecord | None:
        row = self.store.get(TABLE, program_id)
        return ProgramRecord(**row) if row else None

    def set_status(self, program_id: str, status: str, error: str | None = None,
                   failure_mode: str | None = None) -> None:
        if status not in STATUSES:
            raise ValueError(f"unknown status {status!r}; expected one of {STATUSES}")
        rec = self.get(program_id)
        if rec is None:
            raise KeyError(f"program {program_id!r} not registered")
        rec.status, rec.error, rec.failure_mode = status, error, failure_mode
        self.store.upsert(TABLE, program_id, asdict(rec))

    def pending(self) -> list[ProgramRecord]:
        return [ProgramRecord(**row) for row in self.store.scan(TABLE)
                if row.get("status") != "parity_pass"]
