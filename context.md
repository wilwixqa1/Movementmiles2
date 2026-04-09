# Movement & Miles — Session 24 Context Document
## Date: April 9, 2026 | Version: 23.2.0

---

## 1. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform. This system (Movementmiles2) is the **financial tracking/analytics backend** — it does NOT control the website, checkout flow, or app. Those are managed by Tosh via the ymove platform.

**Tech Stack:** FastAPI (Python), PostgreSQL, hosted on Railway  
**Admin Dashboard:** https://movementmiles2-production.up.railway.app/mm-admin  
**Admin Auth:** Header `X-Admin-Password: mmadmin2026`  
**ymove API Base:** https://v6-beta-api.ymove.app

---

## 2. CURRENT SUBSCRIBER NUMBERS (Post-Session 24)

### Our System:
| Source | Active | Trialing | Total Active |
|--------|--------|----------|--------------|
| Stripe | 866 | 156 | 1,022 |
| Apple | 663 | — | 663 |
| Google | 196 | — | 196 |
| **Total** | **1,725** | **156** | **1,881** |

### Tosh's System (ymove):
| Source | Count |
|--------|-------|
| Stripe | 1,030 |
| Apple | 651 |
| Google | 208 |
| Manual | 11 |
| **Total** | **1,870** |

### Discrepancy Analysis:
| Platform | Us | Tosh | Delta | Explanation |
|----------|-----|------|-------|-------------|
| Stripe | 1,022 | 1,030 | -8 | Minor counting difference (trialing methodology) |
| Apple | 663 | 651 | +12 | BUG: Our sync defaults null provider to "apple" |
| Google | 196 | 208 | -12 | Mirror of Apple issue — 12 subs miscategorized as Apple |
| Manual | 0 | 11 | -11 | We don't track manual users at all |

**Key Insight:** The 11 Manual users in Tosh's system likely have `subscriptionProvider: null` in the ymove API. Our sync defaults null to "apple", so they end up in our Apple count.

---

## 3. CRITICAL BUG: Provider Defaulting

### The Problem:
The ymove member-lookup API returns `subscriptionProvider: null` for ALL users. When our shadow sync pulls the bulk subscriber list, it defaults them all to "apple."

### Why Webhooks Work But API Doesn't:
- **Webhook payload** includes `subscriptionPaymentProvider` — correctly returns "apple", "google", or "stripe"
- **Member-lookup API** has `subscriptionProvider` — always returns null
- These are DIFFERENT field names

### Code Locations That Need Fixing (6 total):
1. Line ~2692 — shadow sync pull_all parsing
2. Line ~3001 — ymove-import-new endpoint
3. Line ~2785 — cross_platform_switchers default
4. Line ~2787 — truly_new default
5. Line ~2693 — Guard clause
6. Line ~3003 — Import guard clause

### Proposed Fix:
Change all 6 locations to default to `"undetermined"` instead of `"apple"`.

---

## 4. UTM ATTRIBUTION

### Status: Blocked — Awaiting Tosh
- All Stripe signups have empty `meta` fields from ymove API
- Our code is correct — data isn't there on ymove's side
- Will planned test signup to verify; check results next session

---

## 5. DATA CLEANUP COMPLETED

### Cross-Platform Dedup:
- 7 legacy Meg imports cancelled (batch: `s24_dedup_apple_imports`)
- 3 genuine cross-platform kept (adrian.mccarthy, bkolp41, jaimelash1)

---

## 6. YMOVE API REFERENCE

### Member Lookup returns:
`subscriptionProvider: null` (ALWAYS null for all users)

### Webhook subscriptionCreated returns:
`subscriptionPaymentProvider: "apple|google|stripe"` (CORRECT)

### Webhook subscriptionCancelled:
Does NOT include provider field

---

## 7. SUBSCRIPTION ID PATTERNS

| Prefix | Source | Origin |
|--------|--------|--------|
| `sub_*` | stripe | Direct Stripe webhook |
| `import_apple_*` | apple | Meg's spreadsheet import |
| `ym_google_*` | google | Initial import (correct) |
| `ymove_new_apple_*` | apple | Sync auto-import (BUG) |
| Numeric | apple | Apple transaction from webhook (CORRECT) |

---

## 8. PROPOSED NEXT STEPS

### Priority 1: Fix Provider Defaulting Bug
Change 6 code locations to default to "undetermined" instead of "apple"

### Priority 2: Questions for Tosh
1. Fix `subscriptionProvider` in member-lookup API
2. Send email list for 11 Manual Active Users
3. Where is UTM data stored? Meta field is always empty

### Priority 3: Backfill existing mismatched records once Tosh provides data

---

## 9. SESSION HISTORY

### Session 24 (April 9, 2026):
- Investigated UTM attribution gap — ymove meta field always empty
- Investigated subscriber count discrepancy with Tosh
- Cancelled 7 duplicate cross-platform records (batch: s24_dedup_apple_imports)
- Discovered provider defaulting bug (null to "apple")
- Confirmed webhook provides correct provider, API does not
- **No code pushed** — only database operations via admin endpoints
