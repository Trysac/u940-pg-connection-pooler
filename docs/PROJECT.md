# Project record

## Objective

PostgreSQL transaction-mode connection pooler speaking the v3 wire protocol, with md5 client auth, bounded wait queues and server reuse at idle ReadyForQuery

## Scope

- In scope: the public API documented in [README.md](../README.md), documented and tested.
- Out of scope: anything that needs dependencies or I/O without an accepted ADR.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 3002646 / 41782 |
| Hours (human) | pending measurement |
| Cost (USD) | 1.3010 |

## Factory run u940 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-17 | generation | google/gemini-4-argon | yes | 6343 | 34167 | 0 | 0 | 0.7087 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 168425 | 3838 | 0 | 1125873 | 0.2252 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 202006 | 1215 | 0 | 812990 | 0.2170 |
| 2026-11-17 | threat-model | google/gemini-3.8-flash | no | 131050 | 897 | 0 | 531622 | 0.1415 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 24337 | 1665 | 0 | 0 | 0.0086 |

Total: $1.3010. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
