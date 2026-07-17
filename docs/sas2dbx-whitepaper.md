# Provable Parity: An Agentic Approach to Migrating 1,000 SAS Users to Databricks

**A technical white paper on the sas2dbx migration agent**

*July 2026*

---

## Executive Summary

Enterprise SAS-to-Databricks migrations fail for a predictable reason: not because code cannot be translated, but because nobody can *prove* the translated code produces the same numbers. A finance team that has run the same SAS job for fifteen years will not accept "the new version looks right." They will accept a cell-by-cell comparison against the outputs SAS actually produced.

sas2dbx is a migration agent built around that observation. It uses large language models — accessed exclusively through the company AI gateway — to translate SAS programs into Spark SQL and PySpark, but it never asks anyone to trust the translation. Instead, every converted program is executed in an isolated sandbox against the same input data SAS consumed, and its outputs are diffed cell-by-cell against SAS's ground-truth outputs. Programs that match receive a **parity certificate** — an audit artifact recording exactly what was compared, at what tolerance, by what method. Programs that don't match are repaired automatically by feeding the concrete difference back to the model, up to a bounded number of attempts, and then routed to a human triage queue with full diagnostic context.

The central design thesis: **the parity harness is the product; the LLM is a replaceable component.** Translation quality determines how *fast* programs reach parity, but the validation loop determines whether the migration can be *trusted* — and trust, not speed, is what decides whether 1,000 users actually move.

The system runs entirely inside Databricks notebooks, requires zero packages beyond what the Databricks Runtime already ships (a hard constraint under our JFrog dependency governance), and confines all company-specific integration to two functions and a handful of configuration values. It shipped with 95 automated tests, including a validator self-test suite that injects known corruptions into ground truth and proves the comparison engine catches every one.

---

## 1. The Problem

### 1.1 The shape of an enterprise SAS estate

Our migration covers roughly 1,000 users whose workloads span four decades of SAS idiom:

- **DATA steps and PROC SQL** — the vast majority. Relational transformations: joins, filters, derived columns, aggregations.
- **Macro programs** — `%macro` definitions, `%let` variables, `%include` files that generate code at run time.
- **Reporting PROCs** — MEANS, FREQ, SUMMARY, TRANSPOSE, with SAS-specific default behaviors that silently shape output.
- **Statistical PROCs** — a long tail of REG, LOGISTIC, and friends, where bit-identical output is not achievable even in principle.

The good news: SQL-shaped logic maps cleanly onto Spark, and for it, *exact* output parity is an achievable standard. The bad news is everything else about the problem.

### 1.2 Why the obvious approaches fail

**Manual rewriting** does not scale. At even a day per program — optimistic once testing is included — a thousand-user estate represents years of engineering effort, performed by people who must be fluent in both a dying language and its replacement.

**Rule-based transpilers** promise determinism but demand a near-complete SAS grammar before converting program one. Real-world SAS — macro logic interleaved with data steps, code generated at run time, `%include` chains — punishes parsers. Months of parser investment buys coverage of the code you sampled, not the code you haven't seen yet.

**Naive LLM translation** — paste SAS in, get PySpark out — produces plausible code with unverified semantics. LLMs reliably miss exactly the quirks that matter: SAS counts dates from January 1, 1960 (Unix from 1970); SAS missing values (`.`) sort *lower* than any number and participate in comparisons, while Spark NULLs propagate; `PROC SORT NODUPKEY` keeps the *first* duplicate deterministically, while Spark's `dropDuplicates` keeps an arbitrary one; character comparisons ignore trailing blanks in SAS but not in Spark. Each of these produces output that is subtly, silently wrong — the most expensive kind of wrong, because it surfaces months later in a regulatory report.

### 1.3 The actual problem is trust

All three approaches share a failure mode: they end with someone asserting, rather than demonstrating, that the new code is equivalent. The migration bottleneck is not translation throughput. It is the sign-off conversation with a program owner who bears the risk of being wrong.

That reframing drives the whole design. The question is not "how do we translate SAS well?" It is "what evidence would let a program owner sign off without reading a line of the new code?"

---

## 2. The Solution: Empirical Parity as the Contract

### 2.1 Defining parity precisely

A converted program is **at parity** when every output table matches its SAS ground-truth counterpart under these rules:

- **Order-insensitive.** Tables are compared as multisets of rows — joined on business keys when known, compared by content hash otherwise. Row order is an implementation detail of both engines.
- **Exact match on non-float columns.** Strings, integers, dates: equal or not.
- **Relative tolerance on floats,** default `1e-9`. SAS and Spark legitimately differ in floating-point summation order; demanding bit-identity on doubles would fail correct code. Statistical PROCs may use a looser, per-program tolerance — but every tolerance used is recorded on the certificate. *There is no silent leniency.*
- **Normalization applied to both sides before diffing.** SAS `.` missings become NULL, SAS epoch dates become ISO dates, trailing-blank padding is trimmed — on the ground truth *and* the candidate output identically. We compare meaning, not encoding.

The certificate records, per table: rows compared, the comparison method actually used (keyed with tolerance, or keyless content-hash — including when a duplicate-key fallback silently changed the method), the tolerance applied, and content hashes of the input snapshots. An auditor a year later can reconstruct exactly what was proven.

### 2.2 Architecture: deterministic pipeline, bounded agency

The system is "agentic" in precisely one place — and deliberately boring everywhere else. Control flow is deterministic Python; the LLM is invoked at exactly two points: *translate this step* and *fix this code given this evidence*.

```
inventory (Delta state table, resumable)
  → land inputs + ground truth   (snapshot, normalize SAS quirks, content-hash)
  → preprocess                   (resolve %include, expand %let, split at
                                  DATA/PROC boundaries — deterministic, no parser)
  → translate                    (LLM via gateway: step → Spark SQL, PySpark fallback)
  → execute                      (per-program sandbox schema, write-guarded)
  → validate                     (order-insensitive diff vs. ground truth)
      → pass:  parity certificate
      → fail:  feed diff/traceback to LLM, repair, retry within budgets
      → budgets exhausted: triage report → human queue
```

Twelve focused modules implement this: configuration, gateway client, state store, inventory, data landing, preprocessor, translator, validator, executor, repair loop, reporter, and orchestration — driven by two notebooks: `Migrate_Batch` for the central migration team (walks the inventory with checkpointing; a killed run resumes where it left off) and `Migrate_One` for power users (widget-driven, one program end-to-end).

Why not a free-form multi-agent system, where the model plans its own actions? Because a migration that certifies parity for regulators needs reproducible behavior, attributable failures, and predictable cost per program. Open-ended agency is a liability here. We spend the model's intelligence where it compounds — translating semantics and diagnosing diffs — and spend ordinary software engineering everywhere trust is required.

### 2.3 Step-scoped translation

Programs are split at DATA-step and PROC boundaries before translation, for three reasons that survived contact with review:

1. **Accuracy.** Small, focused prompts outperform 800-line transcription tasks. Mechanical work the LLM is bad at — tracking forty macro variables, inlining include files — is done deterministically before the model ever sees the code.
2. **Attributable repair.** When output table X diverges, we re-prompt the one step that produced X, not the whole program.
3. **Context is preserved, not lost.** Each step's prompt includes the full original program as reference, the schemas of available tables, the libref-to-catalog mappings, the sandbox schema it must write to, and — critically — the *exact names of the required output tables*, so the naming contract between translator and validator cannot drift.

Every translation prompt also carries a cribsheet of SAS↔Spark semantic traps (date epochs, missing-value comparisons, implicit RETAIN, FIRST./LAST. processing, NODUPKEY determinism, trailing blanks, implicit type conversion). This converts institutional knowledge from tribal to systematic: the hundredth program benefits from every trap discovered in the first ten.

### 2.4 The repair loop: two failure classes, two budgets

"Doesn't run" and "runs but doesn't match" are different failures with different signals and different costs, so they get separate budgets:

- **Inner loop — make it run** (per step, ≤3 attempts): a syntax error or runtime exception produces a traceback; the model receives the code plus the traceback and returns a fix. Cheap, fast signal.
- **Outer loop — make it match** (per program, ≤5 attempts): all steps ran but an output diverged; the model receives the implicated step's code plus a structured diff report — row counts, per-column mismatch rates, sample mismatched cells (`id=2: balance 200.5 != 999.9`). Expensive signal, precisely targeted. Each outer attempt rebuilds the sandbox from scratch and grants a fresh inner budget.

The model repairs against *evidence*, not intuition — the same discipline we would demand of a human debugging the migration. When either budget is exhausted, the program routes to triage with the failure mode recorded (`never_ran` vs. `diverged` vs. `budget`), because those are different queues of human work: the first is usually a translation-capability gap, the second a semantic subtlety worth an engineer's attention, the third a runaway program that needed a cap.

Token budgets bound cost at two levels — per program (500K tokens) and per batch run (20M) — and hitting a cap is itself a triage outcome, never a silent continuation.

---

## 3. Safety and Governance

### 3.1 The sandbox: generated code is untrusted code

LLM-generated code executes with real cluster privileges against real schemas, so the executor treats it as untrusted. Each program runs in its own `sandbox_<program_id>` schema; a static guard scans every step before execution and blocks writes targeting any other schema, plus any statement that would change the execution context (`USE`, `DROP/CREATE SCHEMA`, `setCurrentDatabase`) out from under the guard.

The guard's design principle, forged through three adversarial review rounds: **prefer a false positive over a bypass.** It scans both the raw code and a normalized copy (backticks stripped, comments removed, spaced qualified names collapsed) and unions the findings — so a normalization bug can only ever *add* detections, never hide one. Review probing closed bypass classes including backtick-quoted identifiers, `CREATE TABLE IF NOT EXISTS` (whose target the original regex misparsed), `UPDATE`/`DELETE FROM` (routine in translated PROC SQL), f-string-wrapped PySpark writer calls, and comment-obscured targets. A false block costs one repair-loop retry; a bypass costs ground truth. The asymmetry dictates the design.

### 3.2 Minimal integration surface

Everything company-specific is confined to: two methods on the gateway client (`_build_request`/`_parse_response`, which map to the internal REST contract), a secrets scope, and configuration values (catalog names, model identifiers). Model choice is configuration, not code — the system currently defaults to the strongest gateway-approved model for translation and repair, with a second model selectable per stage for future use (for example, a second opinion on triage cases). Development and the entire test suite run against a scripted `MockGateway`; no company data or credentials ever leave the tenant.

The gateway client itself is built for a *shared* enterprise resource: exponential-backoff retries, a circuit breaker that halts the batch after consecutive failures rather than hammering a struggling service, and per-call logging (model, tokens, latency, purpose, program) to a Delta table — including failed calls, which are exactly the ones an operator needs to see.

### 3.3 Zero-dependency runtime

Under JFrog governance, every new package is an approval risk on the critical path. The pipeline therefore uses only what the Databricks Runtime already ships: `pyspark`, `pandas`, and the standard library. The single genuinely useful third-party package (`pyreadstat`, for reading `.sas7bdat` directly) is optional behind a guarded import whose error message names the zero-dependency fallback: export from SAS as CSV/Parquet. Test tooling (`pytest`, local-mode Spark) never touches the cluster.

### 3.4 Audit trail by construction

Every state transition, every LLM call, every repair attempt, and every comparison result lands in Delta tables. The parity certificate and the triage report are generated from that state, not written by hand. When a program owner signs off, the artifact behind the signature is mechanical, reproducible, and stored next to the data it describes.

---

## 4. Testing the Tester

A validation harness that itself contains bugs is worse than no harness: it manufactures false confidence at scale. Two practices addressed this.

**Corruption injection.** The spec requires — and the test suite implements — a validator self-test: take a copy of ground truth, inject known corruptions (a dropped row, a float perturbed just beyond tolerance, a swapped categorical value), and prove the diff engine catches every one, with the right counts and usable samples.

**Adversarial review.** The system was built task-by-task, each task passing an independent spec-compliance and code-quality review before the next began, with a whole-branch review at the end. This process caught defects that would have silently corrupted the migration's core guarantee, including in code the original design itself specified:

- The keyed comparison produced **false failures on duplicate keys**: a full outer join on a non-unique key generates a Cartesian product, reporting mismatches between two byte-identical tables. SAS-derived tables frequently lack unique keys; the fix falls back to exact multiset comparison and *records the method change on the certificate*.
- The SQL tolerance predicate used `rel·max + abs` where the reference implementation used `max(rel·max, abs)` — two subtly different definitions of "close enough" in one codebase, invisible to every existing test, caught by hand-tracing and pinned by a test whose input discriminates the two formulas.
- The certificate template hardcoded the literal `"PASS"` per table rather than deriving it from the comparison result — harmless under current call discipline, and precisely the kind of "trust me" shortcut the artifact exists to eliminate.

The meta-lesson for teams building similar systems: the components that *produce* trust (validators, certificates, sandboxes) deserve the most hostile review, because their failure mode is not a crash — it is a confident, wrong answer.

---

## 5. Operating Model

**Hybrid by design.** A central migration team drives bulk conversion through `Migrate_Batch`; the inventory is idempotent and resumable, so runs can be killed and restarted without rework, and programs already at parity are never reprocessed. Power users self-serve individual programs through `Migrate_One` with guardrails (sandbox-only writes, token caps, tolerance overrides that are recorded, not hidden).

**Golden set before fleet.** Before pointing the system at the estate, a set of representative programs with known outputs serves as the regression suite — and doubles as the in-tenant verification pass for the few components that can only be exercised on a real cluster (Delta MERGE writes, the execution-timeout cancel path).

**Triage as a first-class output.** The system's honest promise is not "every program converts automatically." It is: every program either receives a machine-checked parity certificate, or arrives in a human queue with the closest attempt's code, the concrete evidence of divergence, and a classified failure mode. Human effort is spent exclusively on the residue that genuinely needs judgment — and the cribsheet grows with every case resolved.

---

## 6. Limitations and Roadmap

Honesty about boundaries is part of the trust story:

- **Ground truth is point-in-time.** Certificates are relative to content-hashed input snapshots. If a live source drifts after snapshotting, the certificate still holds for what it claims — but the claim must be read precisely.
- **Statistical PROCs get tolerance, not identity.** Iterative optimizers on different linear-algebra stacks will not match to 1e-9. Per-program tolerances make this explicit rather than pretending otherwise.
- **The sandbox guard is static analysis, not a security boundary of last resort.** It is honest, tested, and fail-closed within its model (a table name computed at runtime in a variable is invisible to it); defense in depth ultimately comes from Unity Catalog permissions on the executing principal.
- **Keyless comparison approximates float tolerance** at ten significant digits via normalized hashing; keyed comparison applies true relative tolerance. The certificate says which ran.
- **Not yet in scope:** orchestration of converted code post-migration (Jobs/Workflows), ODS report-layer fidelity, and interleaved translate-execute so later steps see the actual schemas earlier steps produced (today they see declared names plus the full program).

The most valuable near-term extension is the flywheel: triage resolutions feeding the cribsheet, and the two-model gateway enabling automatic second opinions on programs the primary model cannot bring to parity.

---

## 7. Conclusion

Migrations of this scale live or die on a single question: *why should anyone believe the new numbers?* sas2dbx answers it structurally. LLMs do what they are uniquely good at — translating the semantics of a forty-year-old language and diagnosing concrete diffs — inside a deterministic harness that does what enterprises require: isolate, verify, bound, log, and certify.

The pattern generalizes beyond SAS. Any legacy-modernization effort with executable ground truth — stored procedures, ETL tools, reporting stacks — can adopt the same contract: **let the model write the code; let the harness earn the trust.**

---

*Repository: `sas2dbx` — 12 modules, 2 Databricks notebooks, 95 automated tests. Design spec and implementation plan in `docs/superpowers/`.*
