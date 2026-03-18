# Project Nova

<p align="center">
  <img alt="Project Nova Banner" src="assets/banner.svg" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&amp;weight=600&amp;size=18&amp;pause=900&amp;color=0EA5E9&amp;center=true&amp;vCenter=true&amp;width=900&amp;lines=Local-first+AI+pipeline+for+data+quality+and+safe+remediation;Detect+anomalies+%E2%86%92+cluster+patterns+%E2%86%92+generate+fix+logic+%E2%86%92+apply+with+guardrails;Enterprise+goal%3A+zero-data-loss%2C+auditability%2C+and+compliance-ready+workflows" alt="Typing Animation" />
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white">
  <img alt="Status" src="https://img.shields.io/badge/Status-Phase%202%20%26%203%20Working-22C55E">
  <img alt="Architecture" src="https://img.shields.io/badge/Architecture-6%20Phase-0EA5E9">
  <img alt="Security" src="https://img.shields.io/badge/Security-Air--Gapped%20First-22C55E">
</p>

<p align="center">
  <strong>Semantic anomaly clustering + local SLM remediation for ETL data quality workflows.</strong>
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

- Phase 2 is working end-to-end with embeddings, semantic grouping, pattern reuse, and Chroma persistence.
- Phase 3 is working with local Ollama inference, hybrid retrieval, structured remediation output, and remediation memory write-back.
- The current repo can run a combined Phase 2 -> Phase 3 validation flow locally on the sample anomaly dataset.
- Phase 1, Phase 4, Phase 5, and Phase 6 are still under active development.

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

## Why this project exists

Traditional ETL tools are optimized for data movement but lack semantic understanding of anomalies. When malformed records appear, pipelines either fail or propagate corrupted data downstream. This often forces engineers to manually write SQL or scripts for every recurring issue. This creates three major problems:
- High manual effort for recurring SQL/script-based fixes.
- Throughput issues when anomaly volume spikes.
- Compliance risk when sensitive data is sent to external LLM APIs.

Project Nova addresses this with a **local-first, semantic clustering and explainable remediation architecture** designed to detect anomaly patterns and safely generate transformation logic.

## End-to-End Flow (Simple Language)

1. **Phase 1 - Ingestion (Scaffold / Planned)**
   - Intended to read raw data and perform deterministic validation.
   - Intended to apply schema checks and rule-based anomaly detection.
   - Intended to split input into clean rows and anomaly rows once implemented.
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
4. **Phase 4 - Execution Engine (Scaffold)**
   - Apply approved transformation rules to all rows belonging to a specific anomaly cluster.
5. **Phase 5 - Guardrails (Scaffold)**
   - Enforce confidence checks, risk routing, and quarantine policies.
6. **Phase 6 - Promotion (Scaffold)**
   - Promote validated staging data to production.

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
| Pipeline Orchestrator (`main.py`) | Done | Safe module imports + sequential phase execution |
| Phase 1 Ingestion | Scaffold | Interface/docstring ready, logic pending |
| Phase 2 Clustering | Working | Embeddings, semantic grouping, Chroma persistence, and durable cluster memory |
| Phase 3 SLM Remediation | Working | Local Ollama provider, hybrid retrieval, remediation memory write-back, and audited outputs |
| Phase 4 Execution | Scaffold | Structure only |
| Phase 5 Guardrails | Scaffold | `run(context)` placeholder |
| Phase 6 Promotion | Scaffold | `run(context)` placeholder |
| UI + Tests + Docs | In Progress | Phase 2/3 validation runners and unit tests are available |

## What Works Today

- Semantic clustering of anomaly rows into compact pattern groups.
- Chroma-backed storage for embeddings, cluster memory, and remediation memory.
- Retrieval-aware Phase 3 prompting using static rules plus persisted memory.
- Local Ollama-based remediation generation with structured JSON output.
- Confidence and guardrail-ready metadata in Phase 3 outputs.
- Local validation through unit tests and a combined Phase 2 -> Phase 3 debug runner.

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

# 2) Activate (Windows PowerShell)
.venv\Scripts\Activate.ps1

# 3) Install project dependencies
pip install -r requirements.txt

# 4) Start Ollama and ensure your local model is available
ollama serve
ollama pull llama3.1:8b

# 5) Configure Phase 3 for local SLM
# .env
PHASE3_PROVIDER=ollama
OLLAMA_MODEL=llama3.1:8b
OLLAMA_URL=http://127.0.0.1:11434

# 6) Run pipeline
python main.py
```

## Expected Run Behavior

- The pipeline starts and runs each phase in sequence.
- Implemented phases update the shared `context` object.
- Phase 2 groups anomalies into semantic clusters and persists retrieval memory to ChromaDB.
- Phase 3 consumes Phase 2 clusters and produces structured remediation suggestions using a local Ollama model.
- Scaffold phases still exist for Phase 1, 4, 5, and 6.

## Local Validation

```bash
# Unit validation for Phase 3
pytest tests/test_phase3_slm_remediation.py -q

# Combined Phase 2 -> Phase 3 integration run
python tests/debug_phase23_runner.py
```

Expected current result:
- Phase 2 forms anomaly clusters from the sample dataset.
- Phase 3 returns structured remediations for those clusters.
- The local provider path should show `ollama/<model-name>` in the output summary.

Example validation summary:

```text
Loaded anomalies: 16
Phase 2: 16 anomalies -> 4 clusters
Phase 3: 4 remediations, 0 quarantined, provider=ollama
```

## Design Principles

- **Decoupled pipeline**: anomaly processing should not block ingestion throughput.
- **Air-gapped AI architecture**: anomaly remediation logic is generated using local models to avoid sending sensitive enterprise data to external APIs.
- **Auditability**: every remediation decision should be traceable.
- **Reversibility**: unsafe outputs should be quarantined and replay-safe.

## Roadmap (Practical Next Steps)

- Implement Phase 1 deterministic validator with schema validation, rule checks, and clean/anomaly dataset separation.
- Implement Phase 4 safe transformation executor with rollback metadata.
- Add Phase 5 confidence thresholds + circuit breaker + quarantine flow.
- Add Phase 6 staging tests + production promotion checks.
- Add unit/integration tests around phase-to-phase context contracts.
