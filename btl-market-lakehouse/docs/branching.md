# Branching

`main` intentionally contains only the shared base: README, contracts-independent
common logic, local demo, tests and docs. It is **not** merged with the four role
branches.

Each role owns one large branch. The small task branches are merged into their
large branch, but never into `main` or into another role branch.

```text
main
├── feature/A-ingest
│   ├── feature/A-contracts
│   ├── feature/A-producers
│   └── feature/A-kafka-connect
├── feature/B-streaming
│   ├── feature/B-ohlcv
│   ├── feature/B-indicators
│   ├── feature/B-cassandra
│   └── feature/B-job
├── feature/C-batch
│   ├── feature/C-medallion
│   ├── feature/C-mllib
│   └── feature/C-airflow
└── feature/D-infra
    ├── feature/D-compose
    ├── feature/D-k8s
    ├── feature/D-dashboards
    └── feature/D-ci
```

## Inspect a role

```bash
git branch -a
git log --oneline --graph --all --decorate
git checkout feature/A-ingest
git diff main...feature/A-ingest --stat
```

## Run the shared demo from any branch

All role branches are created from `main`, so they include the shared
`common/` package and the local demo:

```bash
make demo
make test
```

## Merge policy

- Do **not** merge role branches into `main` for this submission.
- If the team later wants an integrated branch, create a separate
  `integration` branch and merge there after review.
- Contracts changes (`contracts/`) must be reviewed by at least one other role
  before merging into a role branch.

## Contract v2 (2026-11-12)

`contracts/` now lives on `main` (contract v2 from the PDF "Kiến trúc – Phân công"), so
the four role branches read one copy. Shared changes flow one way only:

```text
main --merge--> feature/{A-ingest,B-streaming,C-batch,D-infra}
```

Role branches still never merge into `main`. The sub-branches created for the PDF
alignment (one per task, merged `--no-ff` into their big branch) are listed with their
content in `docs/thay-doi-theo-pdf.md`, section 3.
