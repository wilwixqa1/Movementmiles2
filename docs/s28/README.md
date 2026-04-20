# Session 28 — Full reconciliation with ymove and S26 backfill reversal

**Date:** April 14, 2026
**Version at start:** 27.0.0
**Version at end:** 28.0.0
**Disposition:** Cleanup complete. 20 records modified across 4 reversible batches. Dashboard and reconciliation verified.

---

## TL;DR for future Claude sessions

S28 discovered that the S26 backfill was wrong and reversed it. Then it built a ground-truth reconciliation tool (the `s28-full-dump` endpoint) that produces a row-by-row full outer join between our DB and ymove's user table, and used that to categorize every record in the gap between our count and Tosh's count. Then it cleaned up every category that was resolvable on our side, leaving a +7 residual gap that requires Tosh to update records on his side.

**The most important artifact from this session is the full-dump endpoint.** Future reconciliation work should start by running it, not by running ymove-diff. See `endpoints.md` for usage.

**If you are debugging reconciliation in a future session, read these files in order:**
1. `README.md` (this file) — high-level narrative
2. `the-seven-categories.md` — every record type in the gap, with example records
3. `endpoints.md` — what each s28 endpoint does and how to call it
4. `the-s26-backfill-mistake.md` — what went wrong and why
5. `open-questions.md` — things we did NOT resolve and why

---

## What problem we were solving

S27 left us with a reconciliation gap between our dashboard and Tosh's dashboard:

| Source | Ours (start of S28) | Tosh's dash | Delta |
|---|---|---|---|
| Stripe | 1045 | 1023 | +22 |
| Apple | 638 | 657 | −19 |
| Google | 207 | 208 | −1 |
| Total | 1892 | 1888 | +4 |

S27 had hypothesized that the gaps were caused by "phantom sibling records" — fuzzy-matched email typos creating duplicates. S27 also had an active S26 backfill batch of 9 records that had been reactivated as "pending cancel" under the theory that Stripe's period_end semantics were being interpreted wrongly.

**S27 was wrong on both counts.** There were no phantom siblings. The S26 backfill was a mistake. The gap was caused by different things entirely, and uncovering them required building the full-dump tool.

## How we found the truth

### The ymove-diff endpoint wasn't enough

The existing `ymove-diff` endpoint used `status=subscribed` as a filter on ymove's bulk pull. That meant it only saw users ymove currently considered subscribed. Anything ymove had as "previously subscribed," "test account filtered," or classified under different statuses was invisible to our reconciliation.

### The full-dump approach

We built `/api/admin/s28-full-dump` — a read-only endpoint that:
1. Hits ymove's `member-lookup/all` with **no status filter** (gets everything)
2. Pulls every active+tr