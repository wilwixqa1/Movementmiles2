# Movement & Miles — Session 25 Context Document
## Date: April 11, 2026 | Version: 24.1.0

---

## 0. HOW TO WORK ON THIS PROJECT (READ FIRST)

### Repo and Deployment
- **GitHub Repo:** github.com/Wilwixqa1/Movementmiles2 (note: redirects to lowercase wilwixqa1, push still works)
- **Hosting:** Railway (auto-deploys on push to main, takes ~60 seconds)
- **Branch:** main only

### How Claude Pushes Code
Will provides his GitHub PAT in the chat when needed. Claude uses it like this:
```
git remote set-url origin https://PAT@github.com/Wilwixqa1/Movementmiles2.git
git push origin main
```
After every push using a PAT, Claude reminds Will to rotate the token. Never store PAT tokens between sessions, Will rotates them frequently.

### Network Limitations in Claude's Environment
Claude cannot reach the Railway production domain (`movementmiles2-production.up.railway.app`) from its container — it's not in the egress allowlist. This means Claude can't curl admin endpoints or run API tests directly. All testing must happen via Will hitting endpoints in the browser/dashboard. Claude builds dashboard buttons or shows URLs Will can paste into the address bar.

### Development Workflow
1. Will starts a session with the current context file uploaded (or in repo)
2. Claude pulls the repo locally and reads main.py + static/admin.html
3. Claude makes changes, runs `python3 -c "import py_compile; py_compile.compile('main.py', doraise=True)"` to syntax-check
4. Claude commits with a descriptive message starting with "S{N}:" and pushes
5. Will tests via the admin dashboard at https://movementmiles2-production.up.railway.app/mm-admin
6. Will provides feedback or screenshots, Claude iterates

### Directory Structure
```
Movementmiles2/
├── main.py                           # FastAPI backend (~7200 lines)
├── static/admin.html                 # Single-file admin dashboard
├── static/index.html                 # Public landing page (rarely touched)
├── static/css/, static/js/           # Static assets
├── squarespace_utm_script.html       # UTM persistence script for Squarespace
├── context.md                        # THIS FILE
├── requirements.txt
├── Procfile, railway.json            # Railway deployment config
```

---

## 1. CLAUDE'S BEHAVIORAL RULES FOR THIS PROJECT

These rules exist because Will has caught Claude making mistakes in past sessions. They are not optional.

1. **No em-dashes (—) in prose output.** This applies to emails, drafts, conversational text, and anything Will might paste elsewhere. Em-dashes inside code (comments, strings, HTML entities) are fine.

2. **Anti-anchoring rule.** When Will pushes back on the same issue twice, Claude must stop coding, list every assumption it is making, and evaluate each one from scratch. The answer is probably already in the data we have. Don't layer patches on broken assumptions.

3. **Never confirm visual fixes from text extraction alone.** After any visual change, Claude must say "I can't verify this from here, does it look right to you?" and wait for confirmation. No false confirmations.

4. **Be cautious with destructive operations.** Default to soft cancels (status='canceled' with import_batch tag) over hard deletes. Hard delete only when truly garbage data with no historical value. Always preserve created_at, batch trails, and reversibility.

5. **Tone with collaborators.** When drafting emails to Tosh, Ahmed, or Meg, use a conversational, curious tone. Frame findings as "here's what I saw, is this expected?" rather than "you have a bug." Will always reviews before sending.

6. **Cross-reference before reclassifying.** Use the waterfall: existing real records → Meg imports → Stripe API → undetermined. Never guess at provider data.

7. **Present files at session end via the present_files tool.** Don't just commit and walk away — make sure Will can grab the context file from the chat.

8. **Comprehensive means EVERYTHING.** When Will asks for a comprehensive document, include operational workflow, behavioral rules, system state, architecture, and history. Not just project status.

---

## 2. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform (built and run by Meg Takacs of MTFit LLC). This system, **Movementmiles2**, is the **financial tracking and analytics backend**. It does NOT control the website, checkout flow, or app — those are managed by **Tosh Koevoets** via his **ymove** platform.

### Tech Stack
- **Backend:** FastAPI (Python), PostgreSQL, hosted on Railway
- **Frontend:** Single-file admin dashboard (static/admin.html, ~2400 lines)
- **Public site:** Squarespace at movementandmiles.com (Meg owns this)
- **Checkout:** ymove at ymove.app/join/movementandmiles (Tosh owns this)
- **Payments:** Stripe (web), Apple IAP (iOS app), Google Play Billing (Android app)
- **Email:** Resend (daily digest)

### Admin Dashboard Access
- **URL:** https://movementmiles2-production.up.railway.app/mm-admin
- **Auth header:** `X-Admin-Password: mmadmin2026`
- **Browser query auth:** append `?pw=mmadmin2026` for GET endpoints that support it

### ymove API
- **Base URL:** https://v6-beta-api.ymove.app
- **Site ID:** 75
- **Auth header:** `X-Authorization: <YMOVE_API_KEY>` (env var)
- **Key endpoints used:**
  - `GET /api/site/75/member-lookup?email=X` — individual user lookup
  - `GET /api/site/75/member-lookup/all?status=subscribed&page=N` — paginated bulk

### Key People
- **Will Wendt** — developer (you), wilwendt123@gmail.com
- **Meg Takacs** — owner of Movement & Miles (MTFit LLC)
- **Tosh Koevoets** — developer of ymove, controls checkout/app/webhook pipeline
- **Ahmed Abdelrehim** — Marketing Wiz, handles marketing/UTM strategy

---

## 3. DATA INGESTION PIPELINE

There are FOUR ways subscriber data enters our database:

### Path 1: Stripe Webhook (Real-time, Reliable)
- **Endpoint:** `POST /webhooks/stripe`
- **Events handled:** customer.subscription.created/updated/deleted, checkout.session.completed
- **Creates records with:** `stripe_subscription_id` starting with `sub_`, `source='stripe'`
- **Source of truth for:** Stripe subscriber status (we trust this 100%)

### Path 2: ymove Webhook (Real-time, Reliable for Apple/Google)
- **Endpoint:** `POST /webhooks/ymove`
- **Events handled:** subscriptionCreated, subscriptionCancelled
- **Provider field:** `subscriptionPaymentProvider` returns "apple"|"google"|"stripe" correctly
- **Apple records:** numeric transaction ID (e.g., `350003237839176`), `source='apple'`
- **Google records:** `ym_google_<uuid>`, `source='google'`
- **Stripe records:** Stored as event audit only (real Stripe webhook handles the actual record)
- **CRITICAL:** subscriptionCancelled has NO provider field, finds sub by email (Gap 1, see Section 9)

### Path 3: Daily Shadow Sync (8 AM ET, Reconciliation)
- **Function:** `_run_shadow_sync()` and `run_daily_shadow_sync()`
- **What it does:**
  1. Queries all our active Apple/Google/Undetermined subs (`source IN ('apple','google','undetermined')`)
  2. Calls ymove individual `member-lookup` for each, captures status AND `subscriptionProvider` (self-healing)
  3. Pulls all ymove subscribed members via `member-lookup/all` paginated
  4. Computes diff: deactivate (expired), reactivate (cancelled but active in ymove), unknown new users
  5. Auto-applies deactivations and reactivations
  6. For unknown users: runs the **Provider Resolution Waterfall** (see Section 5)
- **Imports unknown users with subscription IDs:** `ymove_new_<provider>_<email_hash>` or `ymove_switch_<provider>_<email_hash>`

### Path 4: Manual Imports (Admin Dashboard)
- **Meg's XLSX:** `import_meg_apple_google` endpoint (legacy data, still callable)
- **CSV imports:** `import_subscribers_csv`, `import_leads_csv`
- **Records created with prefixes:** `import_apple_*`, `import_google_*`, `meg_apple_*`, `meg_google_*`

---

## 4. SUBSCRIPTION ID PATTERNS (Source of Truth Map)

This is THE most important table in this document. The subscription ID prefix tells you the true origin and provider of any record, regardless of what the `source` column says.

| Prefix | True Source | Origin | Confidence |
|--------|-------------|--------|------------|
| `sub_*` | stripe | Direct Stripe webhook | 100% |
| Numeric (`350003237839176`) | apple | Apple transactionId from ymove webhook | 100% |
| `ym_google_<uuid>` | google | ymove webhook with provider=google | 100% |
| `ymove_new_<provider>_*` | unknown | Shadow sync auto-import (provider was guessed) | LOW |
| `ymove_switch_<provider>_*` | unknown | Cross-platform switch import | LOW |
| `import_apple_*` / `meg_apple_*` | apple | Meg's spreadsheet import | Medium |
| `import_google_*` / `meg_google_*` | google | Meg's spreadsheet import | Medium |

When in doubt about a record's true provider, look at the ID prefix, not the `source` column.

---

## 5. PROVIDER RESOLUTION WATERFALL

When the shadow sync (or Provider Cleanup endpoint) finds a user without a known provider, it runs this 5-step waterfall. Same logic in both daily sync and the cleanup endpoint.

```
Step 1: Stripe sub_* duplicate check
  → Does this email already have an active sub_* record?
  → If YES: cancel this synthetic record (it's a duplicate of a real Stripe webhook record)

Step 1b: Apple/Google webhook duplicate check
  → Does this email have an active numeric (Apple) or ym_google_* (Google) record?
  → If YES: cancel this synthetic record (duplicate of real webhook record)

Step 2: Meg import cross-reference
  → Does this email have any import_apple_*, import_google_*, meg_apple_*, or meg_google_* record?
  → If YES: use that source ('apple' or 'google'), Meg's spreadsheet was the original source of truth

Step 3: Stripe API cross-reference
  → Call stripe.Customer.list(email=X) and check for active subscriptions
  → If active Stripe sub found: import as source='stripe' with correct stripe_sub_id

Step 4: Fall through
  → source = 'undetermined'
  → Will be auto-healed if/when Tosh fixes the API
```

---

## 6. CURRENT SUBSCRIBER NUMBERS (Post-S24 Cleanup)

### Our System (1,871 total active+trialing):
| Source | Count |
|--------|-------|
| Stripe | 1,031 (881 active + 150 trialing) |
| Apple | 623 |
| Google | 205 |
| Manual | 9 |
| Undetermined | 3 |
| **Total Active+Trialing** | **1,871** |

### Tosh's System (1,889 total per earlier report, ~36h old):
| Source | Count |
|--------|-------|
| Stripe | 1,022 |
| Apple | 648 |
| Google | 208 |
| Manual | 11 |
| **Total** | **1,859** (per his dashboard) |

### Delta with Tosh: +12 records (0.6%)
- Stripe: +9 (trialing methodology difference)
- Apple: -25 (likely missed webhooks from before shadow sync existed)
- Google: -3
- Manual: -2
- Undetermined: +3 (will heal once Tosh's API populates these 3)
- Note: Tosh's numbers are ~36h old, so real-time delta may be tighter

Note: The data-audit endpoint's `stripe_active` field (881) excludes trialing. The real Stripe active+trialing is 1,031. This is a known display bug (S24 finding, not yet fixed).

---

## 7. ALL ADMIN ENDPOINTS (Built in S23)

### Diagnostic Endpoints (Read-only)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/admin/provider-test` | GET | Tests ymove API provider field across known-good ID patterns + scans all bulk pages |
| `/api/admin/inspect-ymove-user?email=X` | GET | Dump full ymove response for single email (use for UTM/meta debug) |
| `/api/admin/reconciliation-audit` | GET | Batch history, duplicates, source mismatches, data origins, gaps |
| `/api/admin/data-audit` | GET | 15+ data quality checks, MRR confidence scoring |
| `/api/admin/db-check` | GET | Schema verification |

### Action Endpoints (Modify Data)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/admin/provider-cleanup` | POST | Reclassify ymove_new_*/ymove_switch_* via waterfall (preview/apply) |
| `/api/admin/cancel-duplicate` | POST | Safely cancel one duplicate by ID (preserves history) |
| `/api/admin/cancel-all-duplicates` | POST | Bulk cancel all duplicates in one batch |
| `/api/admin/delete-duplicate` | POST | Hard delete (use with caution, has sibling safety check) |
| `/api/admin/revert-batch` | POST | Revert any batch by ID |
| `/api/admin/ymove-shadow-sync` | POST | Manual trigger of shadow sync |
| `/api/admin/ymove-verify` | POST | Various ymove verification modes |

### Dashboard Buttons (in Admin Tools section, 8 total)
1. **Backfill Stripe** (navy)
2. **Reset Test Data** (red)
3. **Import Leads CSV** (green)
4. **Send Test Digest** (green)
5. **Provider Test** (orange) - S23
6. **Reconciliation Audit** (orange) - S23 (includes per-record Cancel/Delete + bulk Cancel ALL button)
7. **Data Audit** (orange) - S23
8. **Provider Cleanup** (red) - S23

---

## 8. TOSH API FIXES — COMPLETED (April 11, 2026)

### Fix 1: subscriptionProvider in member-lookup API ✅ SHIPPED
- Tosh shipped this on April 11, 2026
- Confirmed: 733/735 users in bulk scan return non-null provider (99.7%)
- Our self-healing logic auto-corrected 55 of 58 undetermined records on first sync after fix
- 3 undetermined remain (ymove returns null for these specific users)

### Fix 2: UTM params in meta field ✅ SHIPPED
- Tosh shipped this on April 11, 2026
- Confirmed: test signup (wilwendt123@gmail.com, ID 992032730) returns utm_source, utm_medium, utm_campaign in meta
- Live webhook capture now works for new Stripe signups automatically
- Historical backfill ran: 1,014 active Stripe subs scanned, 6 had UTMs in ymove (rest are pre-UTM-era direct traffic)

### Squarespace UTM Script (Already Working)
- Located at: Squarespace > Settings > Advanced > Code Injection > Footer
- Captures UTMs on landing, persists in 30-day cookies, rewrites all ymove.app links
- Verified end-to-end working
- **UTM tracking scope:** Stripe-only by design. Apple/Google subs go through app stores which strip UTMs. The business strategy is to push users toward Stripe via ymove web checkout, not app store signups.
- **Open question for Ahmed:** are Klaviyo/Facebook campaign links pointing to movementandmiles.com (where the script runs) or directly to ymove.app (where it doesn't)?

---

## 9. KNOWN GAPS AND ARCHITECTURAL RISKS

### Gap 1: Cancellation by email only (LIMIT 1) — UNFIXED
`_ymove_handle_cancelled` finds the most recent active sub by email and cancels it, no source matching. If a user has both active Stripe + active Apple, an Apple cancel webhook could accidentally cancel the Stripe record. Stripe self-corrects since Stripe webhooks are independent, but it's a real risk for cross-platform users.

### Gap 2: Shadow sync only verifies Apple/Google/Undetermined — INTENTIONAL
Stripe drift is invisible to the shadow sync. We rely 100% on Stripe webhooks for Stripe status. This is intentional because Stripe webhooks have built-in retries and are reliable, but it means a missed Stripe webhook would never be caught.

### Gap 3: No webhook retry from ymove — ACCEPTED
If our server is down during webhook delivery, the data is lost. The daily shadow sync catches missing users via the waterfall, but their provider may end up "undetermined" until Tosh's API fix lands.

### Gap 4: Duplicate active emails — CURRENTLY 0
After S23 cleanup, no duplicates remain. Reconciliation Audit + Cancel ALL button can resolve future duplicates one-click.

### Gap 5: Self-healing protection — FIXED IN S23
Shadow sync reactivation now excludes:
- Records with `import_batch LIKE 's23_provider_cleanup%'`
- Records whose email already has an active `sub_*` Stripe sibling

This prevents the morning sync from re-creating duplicates that the cleanup just resolved.

---

## 10. PROPOSED NEXT STEPS

### Immediate
1. Investigate UTM tracking gaps with Ahmed: are Klaviyo/Facebook links pointing to movementandmiles.com or directly to ymove.app?
2. Run UTM backfill on cancelled Stripe subs (`status_filter: "all"`) for historical churn-by-channel analysis when needed
3. Fix data-audit `stripe_active` query to include trialing (one-line fix: `status IN ('active', 'trialing')`)

### Future Improvements
- **Fix Gap 1:** Cancel handler should match by source before fallback to most-recent
- **Stripe reconciliation:** Compare our Stripe count against Stripe API's actual active subs
- **Shadow sync performance:** Currently ~12 minutes for full run. Sequential member-lookup calls in verify phase are the bottleneck. asyncio.gather with semaphore would help.
- **Investigate trialing methodology** with Tosh (9 Stripe delta)
- **Auto-expire on status polls:** Currently the 30-min auto-expire only fires on `action: 'run'`, not `action: 'status'`. Dashboard polling never triggers it.
- **Deprecate Meg's XLSX import endpoint** once confirmed no longer needed

---

## 11. SESSION 24 ACCOMPLISHMENTS (April 11, 2026)

**Major work:**
1. Confirmed both Tosh API fixes working (subscriptionProvider + UTM meta field)
2. Ran shadow sync with self-healing: undetermined dropped from 58 → 3
3. Closed delta with Tosh from 22 records (1.2%) → 18 records (0.95%)
4. Found + fixed Bug 1: cancelled_ag_map source filter excluded 'manual', letting daily sync reactivate manually-cancelled manual duplicates
5. Found + fixed Bug 2: daily sync waterfall had no Step 0 email-existence check, creating duplicate ymove_new_* records when provider segment changed in synthetic ID
6. Built + ran UTM backfill endpoint (POST /api/admin/backfill-utms) with status_filter param
7. UTM backfill result: 1,014 active Stripe subs scanned, 6 had UTMs (rest are pre-UTM direct traffic). Dashboard Marketing & Attribution section now populated.
8. Built cleanup-manual-duplicates endpoint for synthetic-only duplicate case that cancel-all-duplicates can't handle
9. Cleaned 9 manual/manual duplicate records, MRR adjusted down by $179.91/mo (was phantom inflation)
10. Discovered shadow sync takes ~12 minutes (not ~1 hour as previously believed; the "1 hour" was an orphaned sync from a mid-run Railway redeploy)
11. Discovered redeploy-kills-sync footgun: pushing to GitHub during shadow sync kills the background task silently, leaving an orphaned DB row
12. Sent email update to Tosh + Ahmed with UTM tracking status and questions about Klaviyo/Facebook link targets
13. Confirmed MRR Trend chart fix is NOT a TODO (was already handled in S23 via four commits, the "deferred" flag in context was stale)

**Bugs found and fixed (4 total):**
- Bug 1: cancelled_ag_map excluded 'manual' source → daily sync reactivated manual cleanup victims
- Bug 2: daily sync waterfall had no email-existence check → duplicate ymove_new_* records when provider changed
- Bug 3 (noted, not fixed): data-audit stripe_active excludes trialing, Apple/Google include it (inconsistent)
- Bug 4 (noted, not fixed): auto-expire only fires on action:'run', not action:'status'

**Code commits (in order):**
- S24: Fix manual reactivation bug + add UTM backfill endpoint
- S24: Add Step 0 email-existence check to daily sync waterfall
- S24: Add status_filter to backfill-utms (default active+trialing only)
- S24: Add cleanup-manual-duplicates endpoint for synthetic-only case

**Numbers before/after S24:**
- Before: 1,867 active, 58 undetermined, 0 duplicate emails, delta with Tosh: 22
- After: 1,871 active+trialing, 3 undetermined, 0 duplicate emails, delta with Tosh: +12 (Tosh's numbers ~36h old)
- MRR: $33,518.69 → $33,338.78 (removed $179.91 duplicate inflation)
- UTM Attribution: 0 attributed subs → 7 attributed subs (dashboard live)

---

## 12. SESSION HISTORY (Earlier Sessions for Context)

### Session 23 (April 10, 2026) — Provider overhaul + audit tooling

**Major work:**
1. Fixed provider defaulting bug across 8 code locations ("apple" → "undetermined")
2. Built and ran Provider Test v2, definitively confirmed ymove API limitation (290/293 users null)
3. Built self-healing provider logic in shadow sync verify phase (already healed 9 manual users)
4. Built 5-step Provider Resolution Waterfall for unknown users (Stripe dupe → A/G dupe → Meg → Stripe API → undetermined)
5. Built Reconciliation Audit endpoint (batch history, duplicates with delete/cancel buttons, source mismatches, data origins)
6. Built Data Audit endpoint (15+ checks, MRR confidence scoring)
7. Built Provider Cleanup endpoint with preview/apply
8. Added per-record Cancel and Delete buttons + bulk Cancel ALL button
9. Added Provider Test, Reconciliation Audit, Data Audit, Provider Cleanup to dashboard Admin Tools
10. Added "manual" and "undetermined" source types with dashboard styling
11. Shadow sync reactivation now excludes cleanup-cancelled records and active Stripe siblings
12. **Cleaned 79 misclassified records:** 3 Stripe duplicates cancelled, 66 reclassified to undetermined, 10 duplicate emails resolved
13. Verified Squarespace UTM script works end-to-end via test signup
14. Discovered UTM data IS in ymove DB but missing from API response
15. Confirmed `subscriptionProvider` null limitation via test signup
16. Sent Tosh two clear emails with hard evidence (UTM persistence + provider field)
17. **Reduced delta with Tosh from large/unclear to 22 records (1.2%)**

**Numbers before/after S23:**
- Before: ~1,881 active, source breakdown wildly off, confidence LOW
- After: 1,867 active, source breakdown matching Tosh ±1.2%, confidence HIGH

---

## 12. SESSION HISTORY (Earlier Sessions for Context)

### Session 24 (April 9, 2026) — Pre-S23 work
- Investigated UTM attribution gap
- Investigated subscriber count discrepancy with Tosh
- Cancelled 7 duplicate cross-platform records (batch: `s24_dedup_apple_imports`)
- Discovered provider defaulting bug
- No code pushed, only DB operations

### Sessions 16-22 — Foundation
- Built ymove webhook integration (S18 dedup logic)
- Built daily shadow sync (S20)
- Built UTM tracking infrastructure (S21)
- Built admin dashboard buildout
- Built Stripe + Apple + Google webhook handling

### Sessions 11-15 — Initial Build
- Stripe webhook integration
- Trial-to-paid conversion tracking
- Daily digest email system
- Multi-tab XLSX export
- Subscription analytics foundation
