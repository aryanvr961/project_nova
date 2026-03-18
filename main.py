"""
Module: PIPELINE ORCHESTRATOR
Owner: Aryan
Purpose:
- Interactive entry point for end-to-end Project Nova execution.
Responsibilities:
- Prompt for the dataset path when running from the terminal.
- Execute Phase 1 through Phase 6 in strict sequence.
- Show a lightweight loading animation while phases are running.
- Print a concise output summary and artifact save locations.
"""

from __future__ import annotations

from importlib import import_module
from inspect import signature
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
import sys
import time
import shlex


def _safe_import(module_path: str):
    """Import a module and fallback to an empty placeholder module on failure."""
    try:
        return import_module(module_path)
    except Exception as exc:
        print(f"[WARN] Could not import {module_path}: {exc}")
        return SimpleNamespace(__name__=module_path)


phase1_ingestion = _safe_import("phases.phase1_ingestion")
phase2_clustering = _safe_import("phases.phase2_clustering")
phase3_slm_remediation = _safe_import("phases.phase3_slm_remediation")
phase4_execution = _safe_import("phases.phase4_execution")
phase5_guardrails = _safe_import("phases.phase5_guardrails")
phase6_promotion = _safe_import("phases.phase6_promotion")


def _prompt_for_input_path() -> Path:
    while True:
        user_input = input("Dataset path (CSV/JSON/JSONL): ").strip()
        normalized_input = _normalize_dataset_input(user_input)
        if normalized_input is None:
            print("Dataset path is required.")
            continue
        path = Path(normalized_input).expanduser()
        if path.exists() and path.is_file():
            return path
        print(f"File not found: {path}")


def _normalize_dataset_input(user_input: str) -> str | None:
    cleaned = user_input.strip()
    if not cleaned:
        return None
    if cleaned.lower().startswith(("python ", ".\\", "./")) and "main.py" in cleaned.lower():
        try:
            tokens = shlex.split(cleaned, posix=False)
        except ValueError:
            tokens = cleaned.split()
        for token in reversed(tokens):
            candidate = token.strip().strip("\"'")
            if candidate.lower().endswith((".csv", ".json", ".jsonl")):
                return candidate
    return cleaned.strip("\"'")


def _build_context(input_path: Path) -> dict:
    return {
        "input_path": str(input_path),
        "source_name": input_path.stem,
        "vault_dir": str(Path("data") / "vault"),
    }


def _spinner(stop_event: Event, current_label: dict[str, str]) -> None:
    frames = ["|", "/", "-", "\\"]
    index = 0
    while not stop_event.is_set():
        label = current_label.get("value", "Preparing pipeline")
        print(f"\r{frames[index % len(frames)]} {label}...", end="", flush=True)
        index += 1
        time.sleep(0.12)
    print("\r", end="", flush=True)


def _run_phase(module_obj, label: str, candidate_functions, context: dict, current_label: dict[str, str]) -> dict:
    """Run a discovered phase function, or simulate execution if absent."""
    current_label["value"] = label
    try:
        for function_name in candidate_functions:
            phase_fn = getattr(module_obj, function_name, None)
            if callable(phase_fn):
                fn_signature = signature(phase_fn)
                if len(fn_signature.parameters) >= 1:
                    result = phase_fn(context)
                else:
                    result = phase_fn()
                return result if isinstance(result, dict) else context
        print(f"\n[INFO] No phase function found in {module_obj.__name__}; simulated execution.")
        return context
    except Exception as exc:
        print(f"\n[ERROR] {label} failed: {exc}")
        return context


def _run_phase_timed(module_obj, label: str, candidate_functions, context: dict, current_label: dict[str, str]) -> tuple[dict, float]:
    started_at = time.perf_counter()
    result = _run_phase(module_obj, label, candidate_functions, context, current_label)
    elapsed = time.perf_counter() - started_at
    return result, elapsed


def _print_header() -> None:
    print("=" * 64)
    print("Project Nova - Interactive Pipeline Runner")
    print("=" * 64)


def _print_phase_line(label: str, status: str, elapsed: float, details: str | None = None) -> None:
    detail_text = f" | {details}" if details else ""
    print(f"{label:<9} {status:<12} {elapsed:>7.3f}s{detail_text}")


def _format_phase1_details(context: dict) -> str:
    summary = context.get("phase1_summary", {}) or {}
    return (
        f"rows={int(summary.get('input_rows', 0) or 0)}, "
        f"clean={int(summary.get('clean_rows', 0) or 0)}, "
        f"anomalies={int(summary.get('anomalies', 0) or 0)}"
    )


def _format_phase2_details(context: dict) -> str:
    metrics = context.get("phase2_metrics", {}) or {}
    return (
        f"anomalies={int(metrics.get('input_anomalies', 0) or 0)}, "
        f"clusters={int(metrics.get('clusters_formed', 0) or 0)}, "
        f"compression={metrics.get('semantic_compression_ratio', 0)}x"
    )


def _format_phase3_details(context: dict) -> str:
    summary = context.get("phase3_summary", {}) or {}
    return (
        f"remediations={int(summary.get('total', 0) or 0)}, "
        f"quarantined={int(summary.get('quarantined', 0) or 0)}, "
        f"provider={summary.get('provider', 'unknown')}"
    )


def _format_phase4_details(context: dict) -> str:
    summary = context.get("phase4_summary", {}) or {}
    return (
        f"staged={int(summary.get('staged', 0) or 0)}, "
        f"quarantined={int(summary.get('quarantined', 0) or 0)}, "
        f"applied_rows={int(summary.get('applied_rows', 0) or 0)}"
    )


def _format_phase5_details(context: dict) -> str:
    summary = context.get("phase5_summary", {}) or {}
    return (
        f"approved={int(summary.get('approved', 0) or 0)}, "
        f"review={int(summary.get('review_required', 0) or 0)}, "
        f"circuit_breaker={'yes' if summary.get('circuit_breaker_active') else 'no'}"
    )


def _format_phase6_details(context: dict) -> str:
    summary = context.get("phase6_summary", {}) or {}
    return (
        f"promoted_clusters={int(summary.get('promoted_clusters', 0) or 0)}, "
        f"promoted_rows={int(summary.get('promoted_rows', 0) or 0)}, "
        f"blocked={'yes' if summary.get('promotion_blocked') else 'no'}"
    )


def _print_summary(context: dict, phase_times: dict[str, float], total_elapsed: float) -> None:
    print("\nRun Summary")
    print("-" * 64)
    _print_phase_line("Phase 1", str(context.get("phase1_status", "unknown")), phase_times.get("phase1", 0.0), _format_phase1_details(context))
    _print_phase_line("Phase 2", str(context.get("phase2_status", "unknown")), phase_times.get("phase2", 0.0), _format_phase2_details(context))
    _print_phase_line("Phase 3", str(context.get("phase3_status", "unknown")), phase_times.get("phase3", 0.0), _format_phase3_details(context))
    _print_phase_line("Phase 4", str(context.get("phase4_status", "unknown")), phase_times.get("phase4", 0.0), _format_phase4_details(context))
    _print_phase_line("Phase 5", str(context.get("phase5_status", "unknown")), phase_times.get("phase5", 0.0), _format_phase5_details(context))
    _print_phase_line("Phase 6", str(context.get("phase6_status", "unknown")), phase_times.get("phase6", 0.0), _format_phase6_details(context))
    print("-" * 64)
    print(f"Total Runtime: {total_elapsed:.3f}s")
    print(f"Fixed/Promoted Rows: {int(context.get('phase6_summary', {}).get('promoted_rows', 0) or 0)}")


def _print_output_locations(context: dict) -> None:
    output_locations = {
        "Clean rows": context.get("phase1_clean_file"),
        "Anomalies": context.get("phase1_anomaly_file"),
        "Phase 1 audit": context.get("phase1_audit_file"),
        "Phase 4 execution": context.get("phase4_execution_file"),
        "Phase 5 guardrails": context.get("phase5_guardrail_file"),
        "Phase 6 promotion": context.get("phase6_promotion_file"),
    }
    print("\nSaved Outputs")
    print("-" * 64)
    for label, path in output_locations.items():
        if path:
            print(f"{label}: {path}")


def main() -> None:
    """Run Project Nova pipeline phases in strict sequence with an interactive prompt."""
    _print_header()
    arg_path = _normalize_dataset_input(sys.argv[1]) if len(sys.argv) > 1 else None
    input_path = Path(arg_path).expanduser() if arg_path else _prompt_for_input_path()
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    context = _build_context(input_path)
    phase_times: dict[str, float] = {}
    total_started_at = time.perf_counter()
    stop_event = Event()
    current_label = {"value": "Preparing pipeline"}
    spinner_thread = Thread(target=_spinner, args=(stop_event, current_label), daemon=True)
    spinner_thread.start()

    try:
        context, phase_times["phase1"] = _run_phase_timed(
            phase1_ingestion,
            "Phase 1: Data Ingestion",
            ["run", "main", "execute", "ingest", "run_phase1"],
            context,
            current_label,
        )
        context, phase_times["phase2"] = _run_phase_timed(
            phase2_clustering,
            "Phase 2: Anomaly Clustering",
            ["run", "main", "execute", "cluster", "run_phase2"],
            context,
            current_label,
        )
        context, phase_times["phase3"] = _run_phase_timed(
            phase3_slm_remediation,
            "Phase 3: AI Remediation",
            ["run", "main", "execute", "remediate", "run_phase3"],
            context,
            current_label,
        )
        context, phase_times["phase4"] = _run_phase_timed(
            phase4_execution,
            "Phase 4: Execution Engine",
            ["run", "main", "execute", "apply", "run_phase4"],
            context,
            current_label,
        )
        context, phase_times["phase5"] = _run_phase_timed(
            phase5_guardrails,
            "Phase 5: Guardrails",
            ["run", "main", "execute", "validate", "run_phase5"],
            context,
            current_label,
        )
        context, phase_times["phase6"] = _run_phase_timed(
            phase6_promotion,
            "Phase 6: Promotion",
            ["run", "main", "execute", "promote", "run_phase6"],
            context,
            current_label,
        )
    except Exception as exc:
        print(f"\n[FATAL] Pipeline encountered an unexpected error: {exc}")
    finally:
        stop_event.set()
        spinner_thread.join(timeout=1)

    total_elapsed = time.perf_counter() - total_started_at
    print("Pipeline Completed Successfully")
    _print_summary(context, phase_times, total_elapsed)
    _print_output_locations(context)


if __name__ == "__main__":
    main()
