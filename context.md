# Movement & Miles — Session 24 Context Document
## Date: April 10, 2026 | Version: 23.4.0

---

## 1. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform. This system (Movementmiles2) is the financial tracking/analytics backend. It does NOT control the website, checkout flow, or app. Those are managed by Tosh via the ymove platform.

**Tech Stack:** FastAPI (Python), PostgreSQL, hosted on Railway
**Admin Dashboard:** https://movementmiles2-production.up.railway.app/mm-admin
**Admin Auth:** Header `X-Admin-Password: mmadmin2026`
**ymove API Base:** https://v6-beta-api.ymove.app
**Repo:** github.com/Wilwixqa1/Movementmiles2

---

## 2. CURRENT SUBSCRIBER NUMBERS (Post S23 Cleanup)

### Our System (1,867 total active):
| Source | Count |
|--------|-------|
| Stripe | 1,018 |
| Apple | 587 |
| Google | 195 |
| Manual | 9 (auto-healed by shadow sync) |
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

### Delta Analysis (-22 / 1.2%):
- Stripe: -4 (within trialing methodology noise)
- Apple: -61 (most likely sit in our "undetermined" bucket)
- Google: -13
- Manual: -2
- Undetermined: 58 (would distribute across Apple/Google/Manual once Tosh fixes API)

---

## 3. YMOVE API PROVIDER FIELD STATUS

### Confirmed via S23 Provider Test (verified ID patterns + full bulk scan):
- **Individual lookup** (`/member-lookup?email=X`): `subscriptionProvider` is null for nearly all users
- **Bulk lookup** (`/member-lookup/all`): Same. 290/293 users return null. Only 3 users initially returned "manual"
- **Webhook** (`subscriptionCreated`): `subscriptionPaymentProvider` returns apple/google/stripe correctly
- **Webhook** (`subscriptionCancelled`): No provider field

After today's daily sync, our self-healing logic captured 9 manual users (vs the 3 we initially saw), so the API may be slowly populating more.

### UTM Field Status (confirmed via test signup wilwendt123@gmail.com, ymove user ID 992032730):
- UTM params reach ymove checkout URL ✅
- UTM params ARE saved in ymove's database (Tosh confirmed) ✅
- UTM params NOT returned in member-lookup `meta` field ❌
- Tosh is running an update to fix this on his side

---

## 4. S23 CHANGES — COMPLETE LIST

### 4a. Provider Defaulting Bug Fix (8 locations)
Changed null provider defaults from "apple" to "undetermined":
- Shadow sync pull_all parsing (lines 2692-2694)
- cross_platform_switchers and truly_new defaults (lines 2785, 2787)
- ymove-import-new endpoint (lines 3001-3004)
- Downstream import consumers in run_daily_shadow_sync
- Guard clauses now accept: apple, google, stripe, manual, undetermined

### 4b. Self-Healing Provider Logic (Shadow Sync Verify Phase)
During verify phase, if ymove individual lookup returns non-null `subscriptionProvider`, automatically updates our `source` field. Already healed 9 manual users on first run.

### 4c. Provider Resolution Waterfall (Daily Sync + Cleanup)
For unknown users found by sync, follows this order:
1. Stripe sub_* duplicate check → cancel duplicate
1b. Apple numeric / Google ym_google_* duplicate check → cancel duplicate
2. Meg import records → use known source
3. Stripe API cross-reference → use 'stripe'
4. Fall through → 'undetermined'

### 4d. Shadow Sync Reactivation Exclusions
- Excludes records cancelled by `s23_provider_cleanup*` batch
- Excludes any cancelled record where email has active sub_* sibling
- Prevents re-creating duplicates after cleanup

### 4e. Squarespace UTM Persistence (Already Existed)
Existing script at movementandmiles.com Code Injection Footer captures UTMs on landing, persists in cookies (30 days), rewrites all ymove.app links with UTMs appended. Verified working end-to-end via test signup. Three layers: initial decorate, MutationObserver, click handler.

### 4f. New Source Types
- `undetermined` — provider unknown (orange badge)
- `manual` — manually added by Tosh (blue badge)

### 4g. Diagnostic Endpoints + Dashboard Buttons
| Endpoint | Button | Purpose |
|----------|--------|---------|
| GET /api/admin/provider-test | Provider Test (orange) | Tests ymove API provider fields, scans all bulk pages |
| GET /api/admin/reconciliation-audit | Reconciliation Audit (orange) | Batch history, duplicates with delete/cancel buttons, data origins, gaps |
| GET /api/admin/data-audit | Data Audit (orange) | 15+ data quality checks, MRR confidence scoring |
| GET /api/admin/inspect-ymove-user?email=X | (URL only) | Dump full ymove response for one email (used for UTM debug) |
| POST /api/admin/provider-cleanup | Provider Cleanup (red) | Reclassify ymove_new_*/ymove_switch_* with full waterfall |
| POST /api/admin/cancel-duplicate | (per-record button) | Safely cancel one duplicate (preserves history) |
| POST /api/admin/cancel-all-duplicates | Cancel ALL duplicates (red, in audit) | Bulk cancel all duplicate emails in one batch |
| POST /api/admin/delete-duplicate | (per-record button) | Hard delete (use with caution, has safety check) |

### 4h. Cleanup Operations Run Today
1. **Provider Cleanup** (batch `s23_provider_cleanup_20260410_064731`):
   - 3 Stripe duplicates cancelled (bkolp41, jaimelash1, adrian.mccarthy)
   - 66 records reclassified from `apple` to `undetermined`
2. **Bulk duplicate cancel** (batch `s23_provider_cleanup_dedup_*`):
   - 10 duplicate emails resolved
   - Synthetic records cancelled, real webhook records preserved

---

## 5. SUBSCRIPTION ID PATTERNS (Source of Truth)

| Prefix | True Source | Confidence |
|--------|-------------|------------|
| `sub_*` | stripe (direct webhook) | 100% |
| Numeric (e.g. `350003237839176`) | apple (webhook transactionId) | 100% |
| `ym_google_*` | google (webhook) | 100% |
| `ymove_new_*` | unknown (shadow sync guess) | LOW |
| `ymove_switch_*` | unknown (sync switch guess) | LOW |
| `import_apple_*` / `meg_apple_*` | apple (Meg's spreadsheet) | Medium |
| `import_google_*` / `meg_google_*` | google (Meg's spreadsheet) | Medium |

---

## 6. PENDING TOSH FIXES (Both confirmed via test signup)

### Fix 1: Populate `subscriptionProvider` for all users in API
- Currently null for stripe/apple/google in member-lookup responses
- Webhook field works correctly (`subscriptionPaymentProvider`)
- Once fixed, our self-healing logic auto-corrects 58 undetermined records

### Fix 2: Pass UTM params from `meta` field through API responses  
- UTMs ARE saved in ymove DB (Tosh confirmed: `utm_medium`, `utm_source`, `utm_campaign`)
- NOT returned in member-lookup response (only `createdAt` shows)
- Tosh is running an update to fix this

Two emails sent to Tosh (Apr 10):
- UTM persistence with test signup evidence (user ID 992032730)
- subscriptionProvider field follow-up while he's in the API code

---

## 7. KNOWN GAPS AND ARCHITECTURAL RISKS

### Gap 1: Cancellation by email only (LIMIT 1)
`_ymove_handle_cancelled` finds most recent active sub by email and cancels it. If user has both Stripe + Apple active, could cancel wrong one. Stripe self-corrects, but Apple cancel could hit Stripe record incorrectly.

### Gap 2: Shadow sync only verifies Apple/Google/Undetermined
Stripe drift is invisible. Stripe webhook is sole source of truth for Stripe status.

### Gap 3: No webhook retry from ymove
If our server is down during webhook delivery, data is lost. Daily sync catches missing users via the waterfall, but provider may end up "undetermined" until Tosh's API fix lands.

### Gap 4: Duplicate active emails (currently 0 after cleanup)
Unique constraint is on stripe_subscription_id, not email. Use Reconciliation Audit to monitor and bulk-cancel button if needed.

---

## 8. PROPOSED NEXT STEPS

### Immediate (after Tosh fixes API):
1. Run shadow sync manually to trigger self-healing on all 58 undetermined records
2. Verify subscriber counts now match Tosh within ~5 records
3. Restore UTM Attribution dashboard once meta field comes through

### Future improvements:
- Fix Gap 1: cancel handler should match by source before falling back to most-recent
- Stripe reconciliation: compare our Stripe count against Stripe's actual active subs
- MRR Trend chart fix (broken query from earlier S23 TODO, deferred)
- Investigate trialing methodology delta with Tosh (4 Stripe delta)

---

## 9. SESSION 23 HISTORY (April 10, 2026)

**Major accomplishments:**
1. Fixed provider defaulting bug across 8 code locations
2. Built and ran Provider Test v2 — definitively confirmed ymove API limitation
3. Built self-healing provider logic — already healed 9 manual users
4. Built Stripe + Meg + Apple/Google duplicate waterfall for unknown users
5. Built Reconciliation Audit endpoint with batch history, duplicates, data origins, source mismatches
6. Built Data Audit endpoint (15+ checks)
7. Built Provider Cleanup endpoint with preview/apply
8. Added Cancel + Delete duplicate buttons (per-record and bulk)
9. Added Provider Test, Reconciliation Audit, Data Audit, Provider Cleanup, Cancel ALL buttons to dashboard
10. Added "manual" and "undetermined" source types with dashboard styling
11. Shadow sync reactivation now excludes cleanup-cancelled records and active Stripe siblings
12. Cleaned 69 misclassified records (3 cancelled Stripe dupes + 66 reclassified to undetermined)
13. Bulk cancelled 10 duplicate emails
14. Verified Squarespace UTM script works end-to-end via test signup
15. Discovered UTM data IS in ymove DB but missing from API response
16. Confirmed subscriptionProvider null limitation via test signup
17. Sent Tosh two clear emails with hard evidence
18. Reduced delta with Tosh from large/unclear to 22 records (1.2%)

**Code commits (in order):**
- S23: Fix provider defaulting bug (8 locations)
- S23: Provider test endpoint v1
- S23: Reconciliation audit + provider cleanup endpoint
- S23: Provider test v2 (verified ID patterns, full bulk scan)
- S23: Add admin dashboard buttons
- S23: Provider pipeline overhaul (self-healing, Stripe cross-ref)
- S23: Meg import cross-reference in waterfall
- S23: Apple/Google duplicate detection
- S23: Remove preview limits
- S23: Add inspect-ymove-user endpoint
- S23: Add delete-duplicate endpoint with sibling safety check
- S23: Add safe Cancel button alongside Delete
- S23: Add bulk Cancel ALL duplicates button
- S23: Shadow sync reactivation exclusions

### Session 24 (April 9, 2026) — for context:
- Investigated UTM attribution gap (ymove meta field always empty)
- Investigated subscriber count discrepancy with Tosh
- Cancelled 7 duplicate cross-platform records (batch: s24_dedup_apple_imports)
- Discovered provider defaulting bug
- Confirmed webhook provides correct provider, API does not
- No code pushed
