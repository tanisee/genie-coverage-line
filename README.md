# Genie coverage line: concept prototype

A product proposal for Databricks Genie: show, under every time-based answer, which dates the answer covers and whether the data actually covers them.

**Live prototype:** open `index.html` (served via GitHub Pages).

## What's here
| Path | What it is |
|---|---|
| `index.html` | Clickable prototype. Left: Genie's real answers from testing. Right: the proposed coverage line and one-click fix. |
| `experiment/genie_coverage_audit.py` | Databricks notebook used for the test: builds four copies of one synthetic sales table (data ending 1 day, 10 days, 21 days and 1 year ago) and asks each the same 12 questions through the Genie Conversation API. |
| `experiment/genie_audit_coded_results.csv` | All 70 runs with Genie's answer, SQL, and how each was scored. |
| `figures/` | Figures used in the PRD. |

## Method in brief
- Same data in every table; only the last date differs.
- Each question asked in a new conversation; four questions repeated to check consistency.
- A fifth space added one instruction ("state the exact date range; say if data ends before today") to test whether Genie can already do this.
- Each answer scored from its SQL: does it cover a different or incomplete period without saying so?

Synthetic data, one session, Oct 21, 2026. Exploratory, not a statistical sample. Concept prototype only; not affiliated with Databricks.
