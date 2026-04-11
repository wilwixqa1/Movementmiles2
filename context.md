# Movement & Miles — Session 24 Context Document
## Date: April 10, 2026 | Version: 23.4.0

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

## 6. CURRENT SUBSCRIBER NUMBERS (Post-S23 Cleanup)

### Our System (1,867 total active):
| Source | Count |
|--------|-------|
| Stripe | 1,018 |
| Apple | 587 |
| Google | 195 |
| Manual | 9 (auto-healed by self-healing logic) |
| Undetermined | 58 |
| **Total Active** | **1,867** (1,716 active + 151 trialing) |

### Tosh's System (1,889 total):
| Source | Count |
|--------|-------|
| Stripe | 1,022 |
| Apple | 648 |
| Google | 208 |
| Manual | 11 |
| **Total** | **1,889** |

### Delta with Tosh: -22 records (1.2%)
- Stripe: -4 (within trialing methodology noise)
- Apple: -61 (most are likely in our 58 undetermined bucket)
- Google: -13
- Manual: -2

The 58 undetermined records would distribute across Apple/Google/Manual once Tosh fixes the API.

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

## 8. PENDING TOSH FIXES (Both Confirmed via Test Signup)

### Fix 1: Populate `subscriptionProvider` in member-lookup API
- **Test:** `wilwendt123@gmail.com`, ymove user ID 992032730
- **Current state:** Returns null for nearly all stripe/apple/google users
- **Webhook field works correctly:** `subscriptionPaymentProvider`
- **Impact when fixed:** Self-healing logic auto-corrects all 58 undetermined records on next daily sync
- **Status:** Email sent April 10, awaiting Tosh

### Fix 2: Pass UTM params from `meta` field through API
- **Test:** Same user 992032730, signed up with `utm_source=test_will&utm_medium=debug&utm_campaign=s23_test`
- **Tosh confirmed:** UTMs ARE saved in his database
- **Current state:** member-lookup `meta` field only returns `{"createdAt": "..."}`, UTMs missing
- **Impact when fixed:** UTM Attribution dashboard populates naturally for new signups
- **Status:** Tosh acknowledged, running an update on his side

### Squarespace UTM Script (Already Working)
- Located at: Squarespace > Settings > Advanced > Code Injection > Footer
- Captures UTMs on landing, persists in 30-day cookies, rewrites all ymove.app links
- Verified end-to-end working — UTMs reach ymove checkout URL correctly
- The bug was on Tosh's API side, not our script

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

### Immediate (when Tosh ships fixes)
1. Manually trigger shadow sync via dashboard
2. Verify the 58 undetermined records get auto-healed into apple/google/manual
3. Check UTM Attribution dashboard for new attributed signups
4. Run Reconciliation Audit to confirm clean state
5. Compare totals to Tosh, should be within 5 records

### Future Improvements
- **Fix Gap 1:** Cancel handler should match by source before fallback to most-recent
- **Stripe reconciliation:** Compare our Stripe count against Stripe API's actual active subs
- **MRR Trend chart fix** (broken query, deferred from earlier S23)
- **Investigate trialing methodology** with Tosh (4 Stripe delta)
- **Deprecate Meg's XLSX import endpoint** once we confirm we'll never use it again

---

## 11. SESSION 23 ACCOMPLISHMENTS (April 10, 2026)

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
