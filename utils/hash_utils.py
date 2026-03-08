"""
Module: HASHING AND IDEMPOTENCY
Owner: Aadyaa
Purpose:
- Generate hashes to avoid duplicate processing.
Responsibilities:
- Create row-level SHA256 hashes.
- Track processed rows.
- Support safe re-execution of pipeline.
"""