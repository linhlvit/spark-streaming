# Agent Guidance for spark-streaming

This repository is a Spark Structured Streaming CDC pipeline from Oracle T24 to Oracle ODS. Keep this file short and use the linked docs for detail instead of duplicating them.

## Start here

- [README.md](README.md) for the high-level repo layout and quick start.
- [jobs/README.md](jobs/README.md) for run commands, packaging, DLQ scripts, and validation flows.
- [docs/DESIGN.md](docs/DESIGN.md) for the technical architecture and behavior decisions.
- [docs/WORKING-AGREEMENT.md](docs/WORKING-AGREEMENT.md) for business rules, test cases, and production gaps.

## Codebase conventions

- `jobs/main.py` is the entrypoint for all streaming jobs. The supported job families are `sync`, `static_join`, `txn_customer_join`, `txn_acct_join`, and `branch_sales_agg`.
- Prefer changing the owning job module first, then update shared helpers in `jobs/core/` only when the behavior is genuinely shared.
- The streaming jobs write to Oracle with `oracledb` and `foreachBatch`/MERGE patterns; do not switch to JDBC sinks.
- Stream-stream join behavior depends on a one-time `txn_acct` bootstrap before the first run. Treat `jobs/tools/bootstrap_txn_acct.py` as part of the workflow, not an optional helper.
- Runtime checkpoint directories under `checkpoints/` are execution artifacts. Do not edit them unless the task is specifically about recovery or checkpoint inspection.

## Validation and troubleshooting

- Use `python3 tools/test_pipeline.py --scenario ...` for end-to-end checks.
- Use `python3 tools/debug_counts.py` and `sql/monitoring.sql` for health and latency checks.
- Use the DLQ retry and cleanup scripts in `jobs/tools/` for recovery work instead of ad hoc database changes.
- When a change touches packaging or deployment, re-check the commands in `jobs/README.md` so the exported `packages.zip` and deploy paths stay consistent.

## Change safety

- Keep schema changes aligned across `jobs/sql/create_target_tables.sql`, the relevant job code, and the validation scripts.
- Preserve the documented separation between runtime streaming jobs and Airflow orchestration in `dags/`.
- Treat the hardcoded values in `jobs/config.py` as POC defaults; if you need production-ready behavior, prefer an environment-driven approach and update the docs accordingly.
