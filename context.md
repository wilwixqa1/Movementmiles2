# Movement & Miles — Session 24 Context Document
## Date: April 10, 2026 | Version: 23.3.0

---

## 1. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform. This system (Movementmiles2) is the **financial tracking/analytics backend** — it does NOT control the website, checkout flow, or app. Those are managed by Tosh via the ymove platform.

**Tech Stack:** FastAPI (Python), PostgreSQL, hosted on Railway  
**Admin Dashboard:** https://movementmiles2-production.up.railway.app/mm-admin  
**Admin Auth:** Header `X-Admin-Password: mmadmin2026`  
**ymove API Base:** https://v6-beta-api.ymove.app

---

## 2. CURRENT SUBSCRIBER NUMBERS (Pre-Cleanup)

### Our System:
| Source | Active | Trialing | Total Active |
|--------|--------|----------|--------------|
| Stripe | 866 | 156 | 1,022 |
| Apple | 663 | — | 663 |
| Google | 196 | — | 196 |
| **Total** | **1,725** | **156** | **1,881** |

**Note:** Apple/Google counts are inflated. Some ymove_new_*/ymove_switch_* records were incorrectly defaulted to "apple". Provider Cleanup (S23) needs to be run to reclassify.

### Tosh's System (ymove):
| Source | Count |
|--------|-------|
| Stripe | 1,030 |
| Apple | 651 |
| Google | 208 |
| Manual | 11 (only 3 have provider populated in API) |
| **Total** | **1,870** |

---

## 3. YMOVE API PROVIDER FIELD STATUS

### Confirmed by S23 Provider Test (v2):
- **Individual lookup** (`/member-lookup?email=X`): `subscriptionProvider` is **null for ALL users except 3 manual users**
- **Bulk lookup** (`/member-lookup/all`): Same — null for 290/293 users, only 3 manual users have `provider: "manual"`
- **Webhook** (`subscriptionCreated`): `subscriptionPaymentProvider` field works correctly — returns "apple", "google", or "stripe"
- **Webhook** (`subscriptionCancelled`): Does NOT include provider field

### Implication:
The only reliable source of provider data is the webhook. The API cannot tell us provider for any non-manual user. Our subscription ID patterns are the most reliable way to determine provider for existing records.

---

## 4. S23 CHANGES — PROVIDER PIPELINE OVERHAUL

### 4a. Provider Defaulting Bug Fix
Changed 8 code locations from defaulting null provider to "apple" to "undetermined":
- Shadow sync pull_all parsing (2 locations)
- cross_platform_switchers and truly_new defaults (2 locations)
- ymove-import-new endpoint (2 locations)
- Downstream import consumers (2 locations)
- Guard clauses now accept: apple, google, stripe, manual, undetermined

### 4b. Self-Healing Provider (Shadow Sync)
During the verify phase, if ymove individual lookup returns a non-null `subscriptionProvider`, we update our record's source to match. Currently only fires for the 3 manual users. Will auto-heal all records if Tosh fixes the API.

### 4c. Stripe Cross-Reference (Daily Sync)
When shadow sync finds unknown users (not in our DB), instead of blindly importing as "undetermined":
1. Check Stripe API for that email
2. If active Stripe sub found, import as source="stripe" with correct stripe_sub_id
3. If no Stripe record, import as source="undetermined" (Apple/Google/Manual)

### 4d. Alert Mode
Daily sync now logs gap reports: "X users in ymove not in our DB. Y confirmed Stripe, Z undetermined."

### 4e. Retroactive Provider Cleanup Endpoint
`POST /api/admin/provider-cleanup` with `{"preview": true/false}`:
- Finds all active `ymove_new_*` and `ymove_switch_*` records
- Cross-references each email against Stripe API
- Stripe users get source="stripe", rest get source="undetermined"
- Dashboard button available in Admin Tools

### 4f. New Source Types
- `undetermined` : provider unknown (ymove API returned null)
- `manual` : manually added by Tosh (confirmed via API)
- Dashboard badges and colors added for both

---

## 5. SUBSCRIPTION ID PATTERNS (Source of Truth)

| Prefix | True Source | Origin | Confidence |
|--------|-----------|--------|------------|
| `sub_*` | stripe | Direct Stripe webhook | 100% |
| Numeric (e.g. `350003237839176`) | apple | Apple transactionId from ymove webhook | 100% |
| `ym_google_*` | google | ymove webhook with provider=google | 100% |
| `ymove_new_*` | unknown | Shadow sync auto-import (provider was guessed) | LOW |
| `ymove_switch_*` | unknown | Cross-platform switch import (provider was guessed) | LOW |
| `import_apple_*` / `meg_apple_*` | apple | Meg's spreadsheet import | Medium |
| `import_google_*` / `meg_google_*` | google | Meg's spreadsheet import | Medium |

---

## 6. DIAGNOSTIC TOOLS ADDED (S23)

All accessible via Admin Dashboard, Admin Tools section:

| Tool | Button | What It Does |
|------|--------|-------------|
| Provider Test | Orange | Tests ymove API provider fields across all pages, uses verified ID patterns |
| Reconciliation Audit | Orange | Full audit: batch history, duplicates, source mismatches, data origins, gaps |
| Data Audit | Orange | 15+ data quality checks with MRR confidence scoring |
| Provider Cleanup | Red | Reclassify ymove_new_*/ymove_switch_* with Stripe cross-reference |

---

## 7. KNOWN GAPS AND RISKS

### Gap 1: Cancellation by email only (LIMIT 1)
`_ymove_handle_cancelled` finds most recent active sub by email. If someone has both Stripe + Apple active, could cancel wrong one. Stripe self-corrects, but Apple cancel could hit Stripe record.

### Gap 2: Shadow sync only verifies Apple/Google/undetermined
Stripe drift is invisible. We rely 100% on Stripe webhooks for Stripe status.

### Gap 3: No webhook retry from ymove
If our server is down during a webhook, the data is lost. Daily sync catches the gap but cannot determine provider (until Tosh fixes API).

### Gap 4: Duplicate active emails possible
Unique constraint is on stripe_subscription_id, not email. Run reconciliation audit to check current state.

---

## 8. PROPOSED NEXT STEPS

### Immediate:
1. **Run Reconciliation Audit** to see current state of duplicates, batch operations, data origins
2. **Run Provider Cleanup (preview first)** to see how many records need reclassification
3. **Apply Provider Cleanup** to fix historical data

### Ask Tosh:
1. **Fix `subscriptionProvider`** in member-lookup API for all users (not just manual)
   - The field exists, it works for 3 manual users, just needs to be populated for stripe/apple/google
   - This single fix makes our shadow sync self-healing for provider data

### Future:
- Fix Gap 1 (cancellation handler should match by source before falling back to most-recent)
- Stripe reconciliation (compare our Stripe count against Stripe actual active subs)
- MRR Trend chart fix (broken query from S23 TODO)

---

## 9. SESSION HISTORY

### Session 23 (April 10, 2026):
- Fixed provider defaulting bug (8 locations: "apple" to "undetermined")
- Built and ran Provider Test v2: confirmed ymove API returns null provider for 290/293 users
- Only 3 manual users have provider populated in API
- Added self-healing provider logic to shadow sync verify phase
- Replaced auto-import with Stripe cross-reference + undetermined import
- Built retroactive Provider Cleanup endpoint with preview/apply and Stripe cross-ref
- Added Reconciliation Audit endpoint (batch history, duplicates, data origins)
- Added Data Audit button, Provider Test button, Reconciliation Audit button, Provider Cleanup button to dashboard
- Added "manual" and "undetermined" as recognized source types with dashboard styling
- Shadow sync now includes undetermined source in verification queries
- **Code pushed, deployed. Provider Cleanup NOT YET RUN. Needs preview then apply.**

### Session 24 (April 9, 2026):
- Investigated UTM attribution gap, ymove meta field always empty
- Investigated subscriber count discrepancy with Tosh
- Cancelled 7 duplicate cross-platform records (batch: s24_dedup_apple_imports)
- Discovered provider defaulting bug (null to "apple")
- Confirmed webhook provides correct provider, API does not
- **No code pushed**, only database operations via admin endpoints
