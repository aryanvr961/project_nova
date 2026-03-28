# Project Nova

<p align="center">
  <img alt="Project Nova Banner" src="assets/banner.svg" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&amp;weight=600&amp;size=18&amp;pause=900&amp;color=0EA5E9&amp;center=true&amp;vCenter=true&amp;width=900&amp;lines=Local-first+AI+pipeline+for+data+quality+and+safe+remediation;Detect+anomalies+%E2%86%92+cluster+patterns+%E2%86%92+generate+fix+logic+%E2%86%92+apply+with+guardrails;Enterprise+goal%3A+zero-data-loss%2C+auditability%2C+and+compliance-ready+workflows" alt="Typing Animation" />
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white">
  <img alt="Status" src="https://img.shields.io/badge/Status-Phase%201--6%20Working-22C55E">
  <img alt="Architecture" src="https://img.shields.io/badge/Architecture-6%20Phase-0EA5E9">
  <img alt="Security" src="https://img.shields.io/badge/Security-Air--Gapped%20First-22C55E">
  <img alt="Tests" src="https://img.shields.io/badge/Tests-35%20Passing-22C55E">
</p>

<p align="center">
  <strong>Deterministic ingestion, semantic anomaly clustering, local SLM remediation, and execution staging for ETL data quality workflows.</strong>
</p>

<p align="center">
  Built to detect recurring anomaly patterns, compress them into actionable clusters, and generate structured remediation logic without relying on external cloud LLMs.
</p>

## What is Project Nova?

Project Nova is an **AI-assisted data observability** and anomaly remediation pipeline designed to safely detect, cluster, and correct problematic records in ETL and data migration workflows.

Core idea:
- Let 99% clean data move through a fast lane.
- Isolate the 1% problematic rows.
- Cluster similar anomaly patterns.
- Use AI to generate deterministic transformation rules, not direct blind edits on the data.
- Apply fixes with guardrails, audit logs, and reversible workflows.

## Current Snapshot

- Phase 1 is working with deterministic ingestion, schema-aware validation, duplicate checks, and anomaly shaping for downstream clustering.
- Phase 2 is working end-to-end with embeddings, semantic grouping, pattern reuse, and Chroma persistence.
- Phase 3 is working with local Ollama inference, deterministic fast-path remediation, lean retrieval, and remediation memory reuse.
- Phase 4 is working as a safe execution staging layer with remediation contract checks, sandboxed lambda validation, staged row transforms, and audit artifacts.
- The current repo can run a full interactive Phase 1 -> 6 flow directly from `main.py`.
- Phase 5 is now implemented as a policy and audit layer for staged execution outputs.
- Phase 6 now promotes only approved candidates into a production-ready payload with final audit reporting.

## Why This Is Interesting

- Instead of fixing anomalies row-by-row, Project Nova clusters similar failures into reusable anomaly families.
- Instead of letting a model edit data directly, it generates structured transformation logic with confidence and audit context.
- Instead of depending on hosted LLM APIs, the current remediation path is aligned to a local-first Ollama workflow.
- Instead of treating retrieval as prompt stuffing, the system persists cluster and remediation memory for future reuse.

## 📄 Research Report & System Architecture

This repository includes a detailed technical research report that outlines the complete architecture and vision behind **Project Nova**. 

**Key highlights of the report:**
- The core concept of AI-assisted Data Observability and anomaly remediation in ETL pipelines.
- Implementation of semantic anomaly clustering using vector embeddings to compress large volumes of data errors into actionable patterns.
- Use of air-gapped Small Language Models (SLMs) to generate deterministic data transformation rules without exposing sensitive data to external APIs.
- How AI is integrated with deterministic validation layers to detect, cluster, and safely remediate data anomalies in modern data pipelines.

[👉 Click here to read the full Technical Research Report](https://docs.google.com/document/d/1cKEBZS5nA8fz5g_W1S8T49I5LjeO96xW/edit?usp=drivesdk&ouid=117337334576397276483&rtpof=true&sd=true)

The report aligns with the implementation currently available in this repository:
- Phase 1 through Phase 4 are implemented and validated locally.
- Phase 5 is implemented for guardrail policy enforcement and audit reporting.
- Phase 6 is implemented for final promotion gating and production-ready payload generation.
- The current codebase is strongest as a working local-first prototype for anomaly detection, clustering, remediation generation, and staged execution.

## Why this project exists

Traditional ETL tools are optimized for data movement but lack semantic understanding of anomalies. When malformed records appear, pipelines either fail or propagate corrupted data downstream. This often forces engineers to manually write SQL or scripts for every recurring issue. This creates three major problems:
- High manual effort for recurring SQL/script-based fixes.
- Throughput issues when anomaly volume spikes.
- Compliance risk when sensitive data is sent to external LLM APIs.

Project Nova addresses this with a **local-first, semantic clustering and explainable remediation architecture** designed to detect anomaly patterns and safely generate transformation logic.

## End-to-End Flow (Simple Language)

1. **Phase 1 - Ingestion (Working)**
   - Read raw input from local JSON, JSONL, or CSV sources.
   - Apply deterministic validation for required fields, typed columns, and duplicate checks.
   - Split data into clean rows and anomaly rows.
   - Emit downstream-safe anomaly records for Phase 2.
2. **Phase 2 - Semantic Clustering (Working)**
   - Convert anomaly rows into text and generate embeddings.
   - Group semantically similar anomalies into clusters.
   - Persist raw embeddings and cluster memory in ChromaDB.
   - Reuse pattern cache to detect repeated anomaly signatures.
   - Store durable cluster metadata for retrieval-aware remediation.
3. **Phase 3 - SLM Remediation (Working)**
   - Normalize Phase 2 clusters into retrieval-aware remediation inputs.
   - Retrieve rule context from static prompts plus Chroma-backed cluster/remediation memory.
   - Send cluster context to a local Ollama SLM.
   - Generate structured transformation logic with confidence, guardrail metadata, and audit-ready outputs.
4. **Phase 4 - Execution Engine (Working)**
   - Validate Phase 3 remediation contracts before staging execution.
   - Safely compile and apply approved remediation lambdas to anomaly-linked rows.
   - Split staged and quarantined remediations.
   - Persist audit-friendly execution artifacts for downstream guardrails.
5. **Phase 5 - Guardrails (Working)**
   - Enforce confidence checks, risk routing, review gates, circuit breaker logic, and quarantine policies.
6. **Phase 6 - Promotion (Working)**
   - Promote only guardrail-approved staging data and write a final promotion artifact.

## Architecture Diagram

```mermaid
flowchart LR
  A[Raw Data] --> B[Phase 1: Deterministic Validation]
  B -->|Clean| C[Clean Output]
  B -->|Anomalies| D[Phase 2: Semantic Clustering]
  D --> E[Pattern Clusters]
  E --> F[Phase 3: SLM Fix Logic]
  F --> G[Phase 4: Execution Engine]
  G --> H[Phase 5: Guardrails]
  H --> I[Phase 6: Promotion]
  H -->|Unsafe| Q[Quarantine]
  I --> P[Production]
```

## Current Implementation Status

| Area | Status | Notes |
|---|---|---|
| Pipeline Orchestrator (`main.py`) | Done | Interactive dataset prompt, loading animation, sequential execution, and terminal summary |
| Phase 1 Ingestion | Working | Deterministic validation, schema-aware rules, duplicate checks, and anomaly shaping |
| Phase 2 Clustering | Working | Embeddings, semantic grouping, Chroma persistence, and durable cluster memory |
| Phase 3 SLM Remediation | Working | Local Ollama provider, hybrid retrieval, remediation memory write-back, and audited outputs |
| Phase 4 Execution | Working | Contract validation, safe lambda staging, quarantining, and execution artifacts |
| Phase 5 Guardrails | Working | Policy checks, circuit breaker, review routing, promotion candidates, and audit artifacts |
| Phase 6 Promotion | Working | Promotion gating, approved-row payload generation, and final audit artifacts |
| UI + Tests + Docs | In Progress | Validation runners and unit tests are available through Phase 4 |

## What Works Today

- Deterministic ingestion with required-field checks, schema hints, and duplicate detection.
- Semantic clustering of anomaly rows into compact pattern groups.
- Chroma-backed storage for embeddings, cluster memory, and remediation memory.
- Retrieval-aware Phase 3 prompting using static rules plus persisted memory.
- Local Ollama-based remediation generation with structured JSON output.
- Confidence and guardrail-ready metadata in Phase 3 outputs.
- Safe Phase 4 execution staging with sandboxed lambda validation and staged row transforms.
- Interactive CLI runner through `main.py` with dataset prompt, runtime summary, and saved output locations.
- Local validation through unit tests and combined debug runners up to Phase 6.

## Repository Layout

```text
project_nova/
|- main.py
|- config.py
|- data/
|- docs/
|- logs/
|- phases/
|- prompts/
|- tests/
|- ui/
`- utils/
```

## Quick Start

```bash
# 1) Create environment
python -m venv .venv

# 2) Install project dependencies
pip install -r requirements.txt

# 3) Start Ollama and ensure your local model is available
ollama serve
ollama pull llama3.1:8b

# 4) Configure Phase 3 for local SLM
# .env
PHASE3_PROVIDER=ollama
OLLAMA_MODEL=llama3.1:8b
OLLAMA_URL=http://127.0.0.1:11434

# 5) Run pipeline with the project virtual environment interpreter
.\.venv\Scripts\python.exe main.py
```

### UI Dashboard

Project Nova now includes a local Streamlit dashboard for upload-driven runs:

```bash
.\.venv\Scripts\python.exe -m streamlit run ui\dashboard.py
```

What the dashboard provides:
- Upload input data in `CSV`, `JSON`, or `JSONL`
- Optionally upload a schema JSON for Phase 1 validation
- Run the full Phase 1 -> 6 flow from the browser
- Inspect phase-by-phase metrics, row previews, and promotion blockers
- Download every generated artifact individually or as a single zipped run bundle

Sample production-like dataset included in the repo:

```text
data/raw/production_like_1000_rows.json
```

## Expected Run Behavior

- The pipeline starts, prompts for a dataset path, and runs each phase in sequence.
- A terminal spinner shows the currently running phase.
- Implemented phases update the shared `context` object.
- Phase 1 reads local input data, applies deterministic validation, and emits clean rows plus anomaly rows.
- Phase 2 groups anomalies into semantic clusters and persists retrieval memory to ChromaDB.
- Phase 3 consumes Phase 2 clusters and produces structured remediations using deterministic fast paths, cache reuse, and local Ollama fallback when needed.
- Phase 4 validates and stages remediation logic against anomaly-linked rows, then writes execution artifacts for downstream guardrails.
- Phase 5 evaluates staged execution records and writes a guardrail audit report.
- Phase 6 promotes approved rows into a production-ready payload and writes a final promotion report.
- The terminal prints a final runtime summary and all saved output locations.

## Local Validation

```bash
# Unit validation for Phase 1
pytest tests/test_ingestion.py -q

# Unit validation for Phase 3
pytest tests/test_phase3_slm_remediation.py -q

# Unit validation for Phase 4
pytest tests/test_phase4_execution.py -q

# Combined Phase 1 -> Phase 2 integration run
python tests/debug_phase12_runner.py

# Combined Phase 2 -> Phase 3 integration run
python tests/debug_phase23_runner.py
```

Expected current result:
- Phase 1 emits deterministic anomaly records in a format Phase 2 can consume.
- Phase 2 forms anomaly clusters from the sample dataset.
- Phase 3 returns structured remediations for those clusters.
- Phase 4 stages valid remediations, quarantines unsafe ones, and writes an execution artifact.
- The local provider path should show `ollama/<model-name>` in the output summary.

Example validation summary:

```text
Phase 1: 5 rows -> 1 clean, 4 anomalies
Phase 2: 4 anomalies -> 1 cluster
Phase 3: 4 remediations, provider=ollama
Phase 4: staged=3, quarantined=1, applied_rows=12
```

## Tests

Current automated validation status:
- Master test suite: `35 passed`
- Covered areas: Phase 1 ingestion, Phase 2 clustering, Phase 3 remediation, Phase 4 execution, Phase 5 guardrails, and Phase 6 promotion
- Test mode note: Phase 3 unit tests use the `mock` provider for deterministic validation

Run the full test suite:

```bash
pytest -q
```

Run phase-specific suites:

```bash
pytest tests/test_ingestion.py -q
pytest tests/test_clustering.py -q
pytest tests/test_phase3_slm_remediation.py -q
pytest tests/test_phase4_execution.py -q
pytest tests/test_phase5_guardrails.py -q
pytest tests/test_phase6_promotion.py -q
```

Run integration/debug flows:

```bash
python tests/debug_phase1_runner.py
python tests/debug_phase12_runner.py
python tests/debug_phase23_runner.py
```

Observed local benchmark snapshot on the current implementation:
- `1000` production-like rows: about `17.5 sec` end-to-end on the optimized local path
- `1000` production-like rows: `863` clean rows, `139` anomalies, `139` promoted rows in the benchmark run
- Main latency now comes primarily from Phase 2 embeddings rather than Phase 3 remediation

## Stress Test Report

Observed local stress-test snapshot on the current implementation:
- Dataset: `100,000` production-style CSV rows
- Runtime: about `183.9 sec` end-to-end on the local full pipeline path
- Input rows: `100,000`
- Anomalies detected: `1,785`
- Clusters formed: `2`
- Remediations generated: `2`
- Guardrail-approved clusters: `0`
- Promoted rows: `0`
- Promotion status: `blocked` because `guardrail_review_pending`

Phase timing snapshot from the stress run:
- Phase 1 Ingestion: about `3.28 sec`
- Phase 2 Clustering: about `117.07 sec`
- Phase 3 Remediation: about `63.58 sec`
- Phase 4 Execution: about `0.006 sec`
- Phase 5 Guardrails: about `0.004 sec`
- Phase 6 Promotion: about `0.001 sec`

Stress-test interpretation:
- Throughput at `100,000` rows is strong for a local-first prototype running validation, embeddings, clustering, remediation, execution staging, and guardrails in one flow.
- The main runtime bottlenecks are currently Phase 2 embedding and clustering work, followed by Phase 3 local SLM remediation.
- The stress run shows that scale throughput is promising, while approval quality and cold-start latency still need optimization before production-ready claims.

## Design Principles

- **Decoupled pipeline**: anomaly processing should not block ingestion throughput.
- **Air-gapped AI architecture**: anomaly remediation logic is generated using local models to avoid sending sensitive enterprise data to external APIs.
- **Auditability**: every remediation decision should be traceable.
- **Reversibility**: unsafe outputs should be quarantined and replay-safe.

## Roadmap (Practical Next Steps)

- Add schema-path prompting and richer CLI configuration options in `main.py`.
- Add API/scheduler triggers on top of the current interactive CLI runner.
- Add deployment, monitoring, and production storage adapters beyond local artifacts.
