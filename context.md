# Movement & Miles -- Session 29 Context Document
## Date: April 21, 2026 | Version: 28.0.0

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

### Network Limitations in Claude's Environment
Claude cannot reach the Railway production domain from its container. All testing must happen via Will hitting endpoints in the browser/dashboard. Claude builds dashboard buttons or shows URLs Will can paste into the address bar.

### Development Workflow
1. Will starts a session with the current context file uploaded (or in repo)
2. Claude clones the repo and reads main.py + static/admin.html
3. Claude makes changes, runs `python3 -c "import ast; ast.parse(open('main.py').read()); print('syntax OK')"` to syntax-check
4. Claude commits with a descriptive message starting with "S{N}:" and pushes
5. Will tests via the admin dashboard at https://movementmiles2-production.up.railway.app/mm-admin
6. Will provides feedback or screenshots, Claude iterates

### Directory Structure
```
Movementmiles2/
  main.py                           # FastAPI backend (~10,787 lines after S28)
  static/admin.html                 # Single-file admin dashboard (~2700 lines)
  static/index.html                 # Public landing page (rarely touched)
  static/css/, static/js/           # Static assets
  squarespace_utm_script.html       # UTM persistence script for Squarespace
  context.md                        # THIS FILE
  docs/s28/                         # S28 session documentation (partially written)
  requirements.txt
  Procfile, railway.json            # Railway deployment config
```

---

## 1. CLAUDE'S BEHAVIORAL RULES FOR THIS PROJECT

These rules exist because Will has caught Claude making mistakes in past sessions. They are not optional.

1. **No em-dashes in prose output.** This applies to emails, drafts, conversational text. Em-dashes inside code are fine.

2. **Anti-anchoring rule.** When Will pushes back on the same issue twice, Claude must stop coding, list every assumption it is making, and evaluate each one from scratch. Don't layer patches on broken assumptions.

3. **Never confirm visual fixes from text extraction alone.** No false confirmations.

4. **Be cautious with destructive operations.** Default to soft cancels with batch tags. Hard delete only when truly garbage data with no historical value.

5. **Tone with collaborators.** When drafting emails to Tosh, Ahmed, or Meg, use a conversational, curious tone. Frame findings as "here's what I saw, is this expected?" rather than "you have a bug." Will always reviews before sending.

6. **Cross-reference before reclassifying.** Use the waterfall: existing real records -> Meg imports -> Stripe API -> undetermined.

7. **Present files at session end via the present_files tool.**

8. **Comprehensive means EVERYTHING.** Operational workflow, behavioral rules, system state, architecture, history.

9. **Verify, don't trust intermediate cleanup outputs.** When a cleanup endpoint reports "X records updated," check whether downstream automation (especially the daily shadow sync) will preserve or revert the change.

10. **Pre-checks save lives.** When inserting new records, always pre-check for existing records with the same email (any status).

11. **NEW IN S28: Don't push code while a long-running endpoint is in flight.** Railway redeploy kills active requests. Ask Will before pushing. S28 killed an 8-minute ymove bulk pull mid-run by pushing while it was running.

12. **NEW IN S28: Don't scope-creep on reconciliation.** The goal is to identify specific records causing the gap, not to build elaborate categorization frameworks. Build the simplest possible tool, look at the data, make decisions. The s28-full-dump endpoint exists specifically because prior attempts were too clever.

13. **NEW IN S28: ymove "manual" does not mean comped.** Per S25 code comment at line ~3062 and confirmed in S28: ymove returns subscriptionProvider="manual" for users whose records were manually edited in their admin tool, NOT because they have comp/free access. These users may still be paying via Apple/Google/Stripe. Do NOT reclassify records as "manual" based on ymove's label alone.

14. **NEW IN S28: Tosh confirmed that users often exist under different emails across providers.** A user might sign up with one email in Stripe checkout and a different (possibly typo'd) email in the ymove app. Before reporting records as "missing from ymove," check for fuzzy email matches and name matches. Alessandra (alessandraclelia.volpato@gmail.com) is confirmed to exist in ymove under a typo'd variant. amets30@yahoo.com is confirmed to exist under a different email entirely.

---

## 2. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform (built and run by Meg Takacs of MTFit LLC). This system, **Movementmiles2**, is the **financial tracking and analytics backend**. It does NOT control the website, checkout flow, or app. Those are managed by **Tosh Koevoets** via his **ymove** platform.

### Tech Stack
- **Backend:** FastAPI (Python), PostgreSQL, hosted on Railway
- **Frontend:** Single-file admin dashboard
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

### Key People
- **Will Wendt** -- developer, wilwendt123@gmail.com
- **Meg Takacs** -- owner of Movement & Miles (MTFit LLC)
- **Tosh Koevoets** -- developer of ymove, controls checkout/app/webhook pipeline
- **Ahmed Abdelrehim** -- Marketing Wiz, handles marketing/UTM strategy

---

## 3. DATA INGESTION PIPELINE

Four ingestion paths:
1. **Stripe Webhook** (real-time, sub_* IDs, source='stripe')
2. **ymove Webhook** (real-time, Apple numeric IDs / ym_google_* / sub_*)
3. **Daily Shadow Sync** (8 AM ET, reconciliation) -- **CURRENTLY PAUSED, SEE SECTION 4**
4. **Manual Imports** (admin dashboard, Meg's XLSX, CSVs)

---

## 4. CRITICAL: SHADOW SYNC IS PAUSED

**The daily shadow sync was intentionally disabled in S28 commit d08dd79.** The scheduler.add_job call for run_daily_shadow_sync is commented out at line ~293-300 of main.py.

**Why it was paused:** S28 cleaned up 18 records (soft-canceled 9 stale apple/google, reversed 9 backfill, relabeled 2). The shadow sync's reactivation logic (`our_cancelled_ag_map` at line ~2974) could undo some of these cleanups because the `s28_cleanup_*` batch tags are not in the exclusion list (only `s23_provider_cleanup%` and `s24_%` are excluded).

**What needs to happen before re-enabling:**
1. Add `s28_cleanup_%` to the import_batch exclusion list in the `our_cancelled_ag` query at line ~2980-2981
2. Verify via the Tosh/Ahmed call that the cleanups are correct and should persist
3. Uncomment the scheduler.add_job block

**Impact of being paused:**
- New Apple/Google users who ymove tells us about via the bulk pull won't be auto-imported
- The ymove webhook pipeline (`_ymove_handle_created`) still works independently, so real-time events are fine
- Stephanie Annand (stephstrengthsomatics@gmail.com) will NOT be auto-picked up until the sync is re-enabled
- Daily digest still runs at 9 AM ET (only the shadow sync at 8 AM ET is disabled)

---

## 5. CURRENT SUBSCRIBER NUMBERS (Post-S28 Cleanup, April 14 2026)

### Our System (verified via dashboard screenshot)
| Source | Count |
|--------|-------|
| Stripe | 1,039 |
| Apple | 629 |
| Google | 207 |
| Undetermined | 2 |
| **Total Active+Trialing** | **1,877** |

### ymove API (bulk pull, no filter, same day)
| Provider | Active Subscriptions |
|----------|---------------------|
| Stripe | 1,025 |
| Apple | 624 |
| Google | 208 |
| Manual | 8 |
| No provider | 2 |
| **Total Active** | **1,867** |

### Tosh's Dashboard (screenshot from same day)
| Provider | Count |
|----------|-------|
| Stripe | 1,023 |
| Apple | 657 |
| Google | 208 |
| Manual | 11 |
| **Total** | **~1,888** |

### THREE-WAY DELTA SUMMARY

There are THREE sources of truth that don't agree with each other:
1. Our database
2. ymove's bulk API (member-lookup/all?status=subscribed)
3. Tosh's dashboard

Key discrepancies:
- **Our DB vs ymove API: +10 total.** Fully characterized per-record (see Section 6).
- **ymove API vs Tosh dashboard: -21.** We have no visibility into this gap. It means Tosh's dashboard counts ~21 records his own bulk API doesn't return. We flagged this to Tosh in the email.
- **Our DB vs Tosh dashboard: Apple is the big concern.** Our 629 vs his 657 = -28 on Apple. But ymove's own API says 624 Apple. So either Tosh's dashboard counts Apple users we both don't have, OR there are ~28 real Apple users we should have but never received webhooks for. This is the main unresolved question.

---

## 6. THE SEVEN CATEGORIES -- EVERY RECORD IN THE GAP EXPLAINED

S28 built a "full-dump" endpoint that does a row-by-row full outer join between our DB and ymove's complete user table. The result is a CSV of 16,859 rows. Every record in the gap between our system and ymove's API falls into one of these categories:

### Category 1: 9 S26 Backfill Records -- REVERSED IN S28
- **What:** 9 Stripe records that S26 reactivated as "pending cancel" because their current_period_end was in the future
- **Why S26 was wrong:** Stripe API confirmed all 9 are status="canceled" with cancel_at_period_end=false and a real canceled_at timestamp. They are fully terminated, not in a grace period.
- **What S28 did:** Reversed them back to canceled, restoring the original canceled_at timestamps
- **Batch tag:** `s28_cleanup_20260414_051450_reverse_backfill`
- **Emails:** annacurtis497, georgiaikonomopoulou, hannahdkendall, jtraub1789, maria.chandlermc5, monicasweigart, pickledpipa, sokolandaria, sondra@pixa.co.za

### Category 2: 6 Historical Stripe Records -- KEEP, REPORT TO TOSH
- **What:** 6 records with real sub_* Stripe IDs, all verified active via Stripe API
- **Why they're in only_in_ours:** ymove has no trace of these emails. But Tosh confirmed that at least Alessandra and amets30 exist in his system under different/typo'd emails. The remaining 4 have NOT been confirmed.
- **Cat 2 name-match diagnostic:** Stripe customer names checked against our entire DB for other records with matching names under different emails. Zero matches found.
- **Emails and names:** ahfouch (Amanda Fouch), alessandraclelia.volpato (Alessandra Volpato), amets30 (Ashley Tyler), andreedesrochers (Andree Desrochers), chloe.levray (Chloe Levray), hstrandness (Haley Strandness)

### Category 3: 2 Stripe Records ymove Has as Users but Not Subscribed -- KEEP, REPORT TO TOSH
- **What:** abbey.e.baier (Abbey Baier) and cassyroop (Cassandra Roop)
- **Tosh context:** Abbey is "collection paused" in Stripe per Tosh. Cassyroop was pending-cancel April 14, likely fully canceled by now.
- **S29 action:** Check cassyroop's current status. She should be canceled.

### Category 4: 7 Stale Apple Records -- SOFT-CANCELED IN S28
- **What:** Meg-imported or reclassified Apple records ymove confirms are no longer subscribed
- **Batch tag:** `s28_cleanup_20260414_051450_stale_apple`
- **Emails:** andrea.nenadic, juliette-bisot, llbankert, morganaj1022, nienkenijp, smb2895, vacantpatient

### Category 5: 2 Stale Google Records -- SOFT-CANCELED IN S28
- **Batch tag:** `s28_cleanup_20260414_051450_stale_google`
- **Emails:** brigidcgriffin, jennifer.miller4

### Category 6: 13 Classification Disagreements -- LEFT ALONE
- **6a (5 records):** Ours=apple, ymove=manual. Left as apple per S25 "manual" finding. Emails: asiemen10, billyjpappas, jaclyn.morrissette, mona@marketingwizusa.com, raneem@marketingwizusa.com
- **6b (2 records):** Relabeled apple->google to match ymove. Batch tag: `s28_cleanup_20260414_051450_relabel_ag`. Emails: chame.abbey, marisol.diaz927
- **6c (3 records):** Ours=stripe (real sub_*), ymove=manual. Left as stripe. Emails: bjarrell, justine.e.murphy, rowkeller32
- **6d (1 record):** Ours=stripe, ymove=google. Left as stripe. Email: alisonvfarmer
- **6e (2 records):** Both sides undetermined/no-provider. Emails: caseycedwards, heatherboonevirtualassistant

### Category 7: 1 Record Only in ymove -- WAITING
- **What:** Stephanie Annand, stephstrengthsomatics@gmail.com, ymove classifies as Stripe
- **Shadow sync is paused** so she won't be auto-imported. Needs manual investigation or re-enabling sync.

---

## 7. WHAT S28 BUILT (Endpoints Added)

| Endpoint | Method | Purpose | Still useful? |
|----------|--------|---------|---------------|
| s28-verify-only-in-ours | POST | Verifies only_in_ours records against Stripe API + ymove single-email | Yes |
| s28-full-dump | POST/GET | Row-by-row full outer join, JSON + CSV output, /tmp cache | **PRIMARY TOOL** |
| s28-test-account-scan | POST | Scans for test email patterns | Returned 0. Low value. |
| s28-ymove-bulk-no-filter | POST | Superseded by s28-full-dump | Superseded |
| s28-cleanup | POST | Consolidated cleanup, preview/apply. Already applied. | Re-runnable |
| s28-recent-stripe | GET | Lists Stripe records created/updated after a timestamp | Useful for drift |

**To run the full dump** (takes ~8 minutes, uses cache on subsequent calls):
```javascript
fetch('/api/admin/s28-full-dump?ymove_status=&use_cache=false', {method:'POST', headers:{'X-Admin-Password':'mmadmin2026'}}).then(r=>r.json()).then(d=>{console.log(JSON.stringify(d.pull_meta, null, 2)); console.log(JSON.stringify(d.counts, null, 2)); window.s28dump = d;})
```

**To download CSV from cache** (instant after initial pull):
```javascript
window.open('/api/admin/s28-full-dump?ymove_status=&use_cache=true&format=csv&pw=mmadmin2026', '_blank')
```

---

## 8. S28 CLEANUP BATCH TAGS (for reversibility)

- `s28_cleanup_20260414_051450_reverse_backfill` -- 9 backfill records reversed
- `s28_cleanup_20260414_051450_stale_apple` -- 7 stale apple soft-canceled
- `s28_cleanup_20260414_051450_stale_google` -- 2 stale google soft-canceled
- `s28_cleanup_20260414_051450_relabel_ag` -- 2 apple->google relabeled

To look up any batch:
```javascript
fetch('/api/admin/s26-batch-lookup?pw=mmadmin2026&batch=BATCH_TAG_HERE').then(r=>r.json()).then(d=>console.log(JSON.stringify(d,null,2)))
```

---

## 9. TOSH'S EARLIER EMAIL -- KEY FACTS

1. **Users often have different emails across providers.** Email-only matching will always miss some.
2. **Alessandra and amets30 specifically** exist in ymove under different/typo'd emails.
3. **"Manual" means manually set active in ymove's dashboard.** Teammates, friends, or Google billing issue. Tosh and Meg are turning remaining ~11 manual users off.
4. **Abbey is "collection paused"** in Stripe.
5. **Cancelled users stay active until billing period ends.** This is correct behavior.
6. **Test users** (sfdasafsaffas@ymove.app, etc.) should no longer be returned by API. Already cleaned from our DB.
7. **Markus Zwigart** typo already fixed in S26.

---

## 10. WHAT NEEDS TO HAPPEN IN S29

### IMMEDIATE: Prep for Tosh/Ahmed call (Will is meeting them tomorrow)

1. **Re-run s28-full-dump** to get fresh numbers (stale by ~1 week). Takes ~8 min.
2. **Compare fresh dump to S28 baseline** to see if anything drifted while shadow sync was off.
3. **Check cassyroop status.** Should be fully canceled by now.
4. **Check Stephanie (stephstrengthsomatics@gmail.com).** Shadow sync paused, so she's almost certainly still missing. May need manual import.
5. **Prepare reconciliation summary for the call.** Key points:
   - We cleaned up 18 records on our side
   - 6 Stripe customers not found in ymove (some under different emails per Tosh)
   - 1 Stripe customer (Abbey) where Stripe collection is paused
   - Apple gap: our 629 vs Tosh dashboard 657 = -28. ymove API says 624. The gap between Tosh dashboard and his own API is on his side, but we may also be missing real Apple users.
   - Remaining delta ~10 records, under 1%, mostly classification disagreements
   - Proposed: walk through remaining records on the call, decide case by case

### BEFORE RE-ENABLING SHADOW SYNC

6. **Add `s28_cleanup_%` to the exclusion list** at line ~2980-2981 in main.py
7. **Uncomment the scheduler.add_job block** at line ~293-300
8. **Fix health endpoint** at line ~6451 to reflect actual job registration state

### BUGS TO FIX (prioritized)

9. **Churn calculation manipulation (S28 finding).** Line ~6071 counts cleanup cancellations as recent churn. Fix: exclude batch-tagged cancellations or add effective_canceled_at field.
10. **Paid conversion calculation** has the same problem.
11. **Bug B:** Shadow sync false-cancels from transient ymove glitches. Cross-check bulk pull before deactivating.
12. **Bug C:** Stale ymove subscribed data. Add Stripe API cross-check for ymove-reported Stripe subscribers.
13. **Gap 1:** _ymove_handle_cancelled cancels by email LIMIT 1, can miss records.
14. **Full-dump email filter:** Excludes null/empty email records (2-record discrepancy vs dashboard).

### LOWER PRIORITY
15. Bug D (ymove pagination ~11/page). Ask Tosh.
16. Bug E (data-audit trialing display). Cosmetic.
17. Bug F (auto-expire on status polls).

---

## 11. SUBSCRIPTION ID PATTERNS

| Prefix | True Source | Origin | Confidence |
|--------|-------------|--------|------------|
| `sub_*` | stripe | Direct Stripe webhook | 100% |
| Numeric | apple | Apple transactionId from ymove | 100% |
| `ym_google_*` | google | ymove webhook | 100% |
| `ymove_new_<provider>_*` | (varies) | Shadow sync auto-import | LOW |
| `ymove_switch_<provider>_*` | (varies) | Cross-platform switch | LOW |
| `import_apple_*` / `meg_apple_*` | apple | Meg's spreadsheet | Medium |
| `import_google_*` / `meg_google_*` | google | Meg's spreadsheet | Medium |

---

## 12. SESSION HISTORY

### Session 28 (April 14, 2026) -- Full reconciliation and S26 backfill reversal
- Built s28-full-dump: row-by-row full outer join (16,859 rows), now the primary reconciliation tool
- Discovered S26 backfill was wrong: all 9 records fully canceled in Stripe. Reversed them.
- Soft-canceled 7 stale Apple + 2 stale Google (ymove confirmed previouslySubscribed)
- Relabeled 2 apple->google to match ymove
- Verified 6 historical Stripe records against Stripe API + ymove. All real paying customers.
- Paused daily shadow sync to protect cleanup
- Email sent to Tosh/Ahmed suggesting a call to resolve remaining discrepancies
- Total active: 1,892 -> 1,877

### Session 27 (April 13, 2026) -- Read-only investigation
- Ran ymove-diff, found Apple-14 gap. No changes made.
- S27's "phantom sibling" hypothesis was wrong (proved in S28)

### Session 26 (April 13, 2026) -- Period-end semantics + backfill
- Built and ran s26-backfill-pending (LATER REVERSED IN S28)
- Fixed Bug B, built batch-lookup, fuzzy-email-match

### Session 25 (April 12, 2026) -- First systematic reconciliation
- Built ymove-diff, fixed 9 mislabeled records, cancelled 5 test/junk
- Reactivated Kelsey, verified 8 historical Stripe records
- Critical: removed 'manual' from self-heal allowed providers

### Session 24 (April 11, 2026) -- Provider cleanup
- Closed delta from 22 to 18. Fixed 2 bugs. Cleaned 9 duplicates.

### Session 23 (April 10, 2026) -- Provider overhaul
- Fixed provider defaulting, built audit tooling, cleaned 79 records. Delta 22.

### Sessions 16-22 -- Foundation (webhooks, shadow sync, UTM, dashboard)
### Sessions 11-15 -- Initial Build (Stripe, trial tracking, digest, XLSX)

---

## 13. KNOWN BUGS

| Bug | Where | Impact | Priority |
|-----|-------|--------|----------|
| Churn manipulation | Line ~6071 | Cleanup cancellations inflate 30-day churn | MEDIUM |
| B | Shadow sync deactivation | False-cancels from transient glitches | HIGH |
| C | Shadow sync import | Stale ymove -> false active records | MEDIUM |
| D | ymove pagination | ~11 users/page suspicious | UNKNOWN |
| E | data-audit endpoint | Trialing display inconsistency | LOW |
| F | Auto-expire on status polls | Only on action:'run' | LOW |
| Gap 1 | _ymove_handle_cancelled | LIMIT 1 email match | MEDIUM |
| Gap 3 | ymove webhook | No retry | ACCEPTED |
| Full-dump filter | s28-full-dump | Excludes null-email records | LOW |
| Health endpoint | Line ~6451 | Misleading while sync paused | LOW |

---

## 14. STANDING REMINDERS

- SimpleBlueprints rules don't apply here, this is Movement & Miles
- All admin endpoint changes need preview/apply pattern, never one-shot writes
- Record IDs to never touch without confirmation: sub_* (Stripe), numeric (Apple), ym_google_* (Google)
- Batch tag pattern `s2X_*_<timestamp>` for reversibility
- Stripe is source of truth for Stripe records
- ymove is upstream for Apple/Google
- Will reviews and sends all external emails. Claude drafts only.
- The s28-full-dump endpoint is the ground truth reconciliation tool. Start there.
- Shadow sync is PAUSED. Must add batch exclusions before re-enabling.
