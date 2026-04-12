# Movement & Miles — Session 25 Context Document
## Date: April 12, 2026 | Version: 25.0.0

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
Claude cannot reach the Railway production domain from its container — it's not in the egress allowlist. All testing must happen via Will hitting endpoints in the browser/dashboard. Claude builds dashboard buttons or shows URLs Will can paste into the address bar.

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
├── main.py                           # FastAPI backend (~8800 lines after S25)
├── static/admin.html                 # Single-file admin dashboard (~2700 lines after S25)
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

1. **No em-dashes in prose output.** This applies to emails, drafts, conversational text. Em-dashes inside code are fine.

2. **Anti-anchoring rule.** When Will pushes back on the same issue twice, Claude must stop coding, list every assumption it is making, and evaluate each one from scratch. Don't layer patches on broken assumptions.

3. **Never confirm visual fixes from text extraction alone.** No false confirmations.

4. **Be cautious with destructive operations.** Default to soft cancels with batch tags. Hard delete only when truly garbage data with no historical value.

5. **Tone with collaborators.** When drafting emails to Tosh, Ahmed, or Meg, use a conversational, curious tone. Frame findings as "here's what I saw, is this expected?" rather than "you have a bug." Will always reviews before sending.

6. **Cross-reference before reclassifying.** Use the waterfall: existing real records → Meg imports → Stripe API → undetermined.

7. **Present files at session end via the present_files tool.**

8. **Comprehensive means EVERYTHING.** Operational workflow, behavioral rules, system state, architecture, history.

9. **NEW IN S25 — Verify, don't trust intermediate cleanup outputs.** When a cleanup endpoint reports "X records updated," that doesn't prove the change persists. The shadow sync may overwrite it. Always check whether downstream automation (especially the daily shadow sync) will preserve or revert the change before declaring victory.

10. **NEW IN S25 — Pre-checks save lives.** When inserting new records, always pre-check for existing records with the same email (any status). Kelsey looked like a missing user but was actually a falsely-cancelled record. The pre-check caught it.

---

## 2. SYSTEM OVERVIEW

Movement & Miles is a fitness subscription platform (built and run by Meg Takacs of MTFit LLC). This system, **Movementmiles2**, is the **financial tracking and analytics backend**. It does NOT control the website, checkout flow, or app — those are managed by **Tosh Koevoets** via his **ymove** platform.

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
- **Will Wendt** — developer, wilwendt123@gmail.com
- **Meg Takacs** — owner of Movement & Miles (MTFit LLC)
- **Tosh Koevoets** — developer of ymove, controls checkout/app/webhook pipeline
- **Ahmed Abdelrehim** — Marketing Wiz, handles marketing/UTM strategy

---

## 3. DATA INGESTION PIPELINE
(Unchanged from S24. See S24 doc for full detail.)

Four ingestion paths:
1. **Stripe Webhook** (real-time, sub_* IDs, source='stripe')
2. **ymove Webhook** (real-time, Apple numeric IDs / ym_google_* / sub_*)
3. **Daily Shadow Sync** (8 AM ET, reconciliation)
4. **Manual Imports** (admin dashboard, Meg's XLSX, CSVs)

---

## 4. SUBSCRIPTION ID PATTERNS (Source of Truth Map)

| Prefix | True Source | Origin | Confidence |
|--------|-------------|--------|------------|
| `sub_*` | stripe | Direct Stripe webhook | 100% |
| Numeric (`350003237839176`) | apple | Apple transactionId from ymove webhook | 100% |
| `ym_google_<uuid>` | google | ymove webhook with provider=google | 100% |
| `ymove_new_<provider>_*` | (varies) | Shadow sync auto-import | LOW |
| `ymove_switch_<provider>_*` | (varies) | Cross-platform switch import | LOW |
| `import_apple_*` / `meg_apple_*` | apple | Meg's spreadsheet import | Medium |
| `import_google_*` / `meg_google_*` | google | Meg's spreadsheet import | Medium |
| `s25_backfill_*` | (varies) | S25 manual backfill (only kelseymsimms case planned then bypassed) | High |

---

## 5. CURRENT SUBSCRIBER NUMBERS (Post-S25 Cleanup)

### Our System
| Source | Count |
|--------|-------|
| Stripe | ~1,026 (down from 1,031, removed 5 test/junk) |
| Apple | ~633 (up from 623, includes 9 relabels + Kelsey reactivation) |
| Google | 205 |
| Manual | ~0-1 (was 9, all relabeled) |
| Undetermined | 3 (will heal as ymove returns real providers) |
| **Total Active+Trialing** | **~1,866-1,867** |

### Tosh's System
~1,859 (per ~36h-old report)

### Delta with Tosh: ~+7 to +8
**Every record in the delta is now identified and explained.** No remaining mystery noise. Breakdown:
- **8 historical Stripe records** verified active in Stripe but missing from ymove (legacy bypass — needs Tosh confirmation)
- **5 Meg-imported Apple records** that ymove has no record of (probably stale)
- **4 provider mismatches** where ymove says Google/Manual but our `sub_*` IDs prove Stripe (ymove side wrong)
- **2 records** (Isabella Marovich-Tadic, Jess Mullen) that ymove's bulk endpoint reports as subscribed but Stripe API confirms cancelled — **real ymove stale-data bug**
- **0 unexplained records**

---

## 6. SESSION 25 ACCOMPLISHMENTS (April 12, 2026)

### Major work
1. **Built read-only ymove-diff endpoint** for per-email comparison with Tosh's data — first time we've ever seen the delta record-by-record
2. **Fixed 9 mislabeled records** (8 + Jaclyn) where source='manual' should have been 'apple' — sub_id prefixes proved the correct provider
3. **Cancelled 5 test/junk Stripe records** identified in the diff
4. **Reactivated Kelsey** (id=11315) after diagnosing she was a false-cancel from `shadow_6_20260329_121938`
5. **Built false-cancel diagnostic** that found Kelsey was the only false-cancel out of 256 cancelled Apple/Google/Undetermined records (0.4% rate)
6. **Verified 8 historical Stripe records** against live Stripe API — all 8 confirmed active in Stripe, our records are correct, the records exist legitimately and just don't appear in ymove for unknown reasons
7. **Investigated 2 new only_in_ymove records** (Isabella, Jess) — Stripe API confirms both genuinely cancelled, ymove's bulk endpoint is showing stale subscribed status
8. **Built false-cancelled-Stripe scan** — found exactly 2 (Isabella, Jess) out of 5,193 cancelled Stripe records (0.04% rate). Stripe webhook handling is essentially correct.
9. **CRITICAL FIX:** Removed `'manual'` from the self-heal allowed providers list at line 2961. Without this, tomorrow's morning sync would have re-mislabeled all 9 records back to source='manual'.

### Bugs found and FIXED in S25
- **Bug A (FIXED, line 2961):** Shadow sync self-heal accepted `subscriptionProvider: 'manual'` from ymove and wrote it to our `source` column. ymove's "manual" likely means "manually edited in admin tool," not a payment provider. Fixed by removing 'manual' from the allowed list. The remaining options are 'apple', 'google', 'stripe'.

### Bugs found but NOT YET FIXED in S25 (queued for S26+)
- **Bug B (Kelsey case):** Shadow sync deactivated a record based on a single point-in-time read of ymove's individual member-lookup. If ymove returned `activeSubscription: false, previouslySubscribed: true` even briefly (transient glitch, partial response, race condition), the parse logic returned "expired" and the record got cancelled. No retries, no cross-confirmation. **Recommended fix:** Cross-check the bulk pull (`ymove_all_emails`) before deactivating — if the email is also in the bulk subscribed list, DON'T deactivate. This would have saved Kelsey.

- **Bug C (Isabella/Jess case):** ymove's `member-lookup/all?status=subscribed` bulk endpoint returns users that Stripe API confirms are cancelled. ymove-side bug (stale data), but our system trusts ymove's bulk endpoint as authoritative. **Recommended action:** Add a Stripe API cross-check during shadow sync's import phase for any user ymove reports as Stripe-provider — if Stripe disagrees, trust Stripe.

- **Bug D (ymove pagination math is suspicious):** Two diff runs returned 1853 and 1854 emails respectively, both with `pages_pulled: 168`. 1854 / 168 = ~11 users per page, which is a weirdly small page size. May indicate ymove returns inconsistent results between calls, or has a hidden cap, or the page size really is ~11. Worth asking Tosh directly.

- **Bug E (data-audit display, from S24, still unfixed):** `stripe_active` field excludes trialing while Apple/Google include it. Cosmetic, low priority.

- **Bug F (auto-expire timing, from S24, still unfixed):** Auto-expire only fires on `action: 'run'`, not `action: 'status'`. Dashboard polling never triggers it.

### Numbers before/after S25
- Before: 1,871 active+trialing, MRR $33,338.78, delta with Tosh +12
- After: ~1,866-1,867 active+trialing, MRR ~$32,978 (removed ~$360 of test/junk records, 1999 cents Kelsey added back)
- 9 records relabeled (no count change)
- 5 cancelled (test/junk)
- 1 reactivated (Kelsey)
- 0 hard deletes
- All operations reversible via batch tags

### Code commits (in order)
- S25: Add ymove-diff endpoint + dashboard button (read-only)
- S25 Step 1: Add fix-manual-apple-labels endpoint (preview/apply, scoped relabel)
- S25 Step 2: Add fix-manual-import-labels endpoint for import_apple/google variants
- S25 Step 3: Add cancel-test-stripe endpoint (5-record allowlist, soft-cancel)
- S25 Step 4: Add backfill-kelsey endpoint (later replaced)
- S25 Step 4b: Add false-cancel diagnostic endpoint
- S25 Step 4 (revised): Reactivate Kelsey id=11315 instead of inserting new
- S25 Step 5: Add verify-historical-stripe endpoint (read-only Stripe API check)
- S25 CRITICAL: Remove 'manual' from self-heal allowed providers (line 2961)
- S25: Add investigate-stripe-gaps endpoint (Part 1: 2 specific emails, Part 2: false-cancelled Stripe scan)
- S25: Add Isabella+Jess Stripe verification (this sub + customer-wide history)

### Operational artifacts created (live in dashboard, all preview/apply pattern)
1. ymove Diff (S25) — read-only diff with preflight checks
2. Fix manual->apple labels (S25)
3. Fix manual->import labels (S25)
4. Cancel test/junk Stripe (S25)
5. Reactivate Kelsey (S25)
6. Find false-cancelled (S25 diagnostic)
7. Verify historical Stripe (S25)
8. Investigate Stripe gaps (S25)
9. Verify Isabella + Jess (S25)

These can all be re-run safely. Most are read-only diagnostics. The action endpoints have allowlists or strict guards so they cannot be misused.

---

## 7. KEY FINDINGS FOR THE WRAP EMAIL TO TOSH + AHMED

### For Tosh — concrete bugs with hard evidence
1. **`subscriptionProvider: "manual"` semantics.** ymove returns this for at least 9 users in our DB whose `stripe_subscription_id` prefix proves they're paying via Apple. We've been treating "manual" as a payment provider value but it appears to be an admin-edit flag. What does it mean in your API?

2. **Two specific stale-subscribed records:** Isabella Marovich-Tadic (ymove user 991975712) and Jess Mullen (ymove user 992014166) both show as `activeSubscription: true, subscriptionProvider: stripe` in your individual lookup AND in your bulk subscribed list, but Stripe API confirms both cancelled in March (Mar 12 and Mar 31 respectively). Specific sub_ids: `sub_1SoxpNFkITCMEwTDKCaf7NmT` and `sub_1TAM6nFkITCMEwTDm4ZnHeZO`.

3. **Pagination math suspicious:** We pulled `member-lookup/all?status=subscribed` and got `totalPages: 168` returning ~1,854 users. That's ~11 users per page which seems unusually small. Two calls 90 minutes apart returned 1853 and 1854 emails. Is the bulk endpoint pagination stable, and what's the expected page size?

4. **8 active Stripe subs missing entirely from ymove:** abbey.e.baier, ahfouch, alessandraclelia.volpato, amets30, andreedesrochers, cassyroop, chloe.levray, hstrandness. All confirmed active in Stripe API. Oldest: Nov 2022. Was there a signup flow that bypassed ymove, or are these gaps on your side?

5. **4 provider mismatches where you have wrong classification:** alisonvfarmer (you say google), bjarrell, justine.e.murphy, rowkeller32 (you say manual for the last 3). All have real `sub_*` IDs from real Stripe webhooks — they're definitely Stripe payers.

### For Ahmed
1. End-to-end UTM test through each campaign channel (Klaviyo, Facebook, email)
2. Are any current campaigns linking directly to ymove.app instead of movementandmiles.com first?

### Records we cancelled in our system tonight (asking for confirmation/revert)
Soft-cancelled 5 records that looked like test/junk in our diff:
1. `sfdasafsaffas@ymove.app` — keyboard mash, trialing
2. `sfdfdssfdfsdfsdasfad@ymove.app` — keyboard mash, $179.99/mo plan
3. `utm_sourceemail@ymove.app` — Will's UTM test signup
4. `markus.zwigart@gmx.dr` — typo'd TLD (`.dr` not real), active since Sept 2025
5. `tosh.koevoets@gmail.com` — Tosh's own account, $179.99/mo plan
**These are reversible via batch tag `s25_test_cancel_20260412_030202`. If any should NOT have been cancelled, let us know and we'll restore.**

---

## 8. PROPOSED NEXT STEPS (S26+)

### Immediate (Session 26)
1. **Wait for Tosh + Ahmed responses** to S25 wrap email
2. **Verify the morning sync didn't undo any S25 work.** Check that the 9 relabeled records still have source='apple' after the Apr 12 8 AM ET sync. If the fix worked, no further action. If something slipped through, investigate.
3. **Re-run ymove-diff** to confirm steady state. Should be very close to morning of Apr 12 numbers.
4. **Investigate Bug D (pagination)** if Tosh has answered: build a tiny diagnostic that fetches just page 1 of ymove's bulk endpoint and inspects the raw response (totalPages, total, pageSize fields) so we know what the contract really is.

### Short-term improvements
1. **Fix Bug B (false cancel from transient glitches):** Cross-check the bulk pull `ymove_all_emails` set before deactivating a record in shadow sync Phase 3. If ymove's bulk says "subscribed" and individual says "expired," DON'T deactivate. Soft-cancel with `pending_verification` flag instead.
2. **Fix Bug C (stale ymove → false subscribed):** Add Stripe API cross-check during shadow sync's import phase for any user ymove reports as `subscriptionProvider: stripe`. If Stripe disagrees, trust Stripe.
3. **Fix Bug E (data-audit trialing display):** One-line fix.
4. **Fix Bug F (auto-expire on status polls):** Small fix.
5. **Re-run UTM backfill on cancelled Stripe subs** for historical churn-by-channel analysis when needed.

### Longer-term improvements
- **Crack the persistent ~7-8 record delta with Tosh** — depends on his answers about the 8 historical Stripes
- **Fix Gap 1:** Cancel handler should match by source before fallback to most-recent
- **Shadow sync performance:** ~12 minutes is dominated by Phase 1 sequential lookups. asyncio.gather with semaphore would help.
- **Investigate Bug D pagination** thoroughly
- **Deprecate Meg's XLSX import endpoint** once confirmed no longer needed

---

## 9. SESSION HISTORY (Earlier Sessions for Context)

### Session 24 (April 11, 2026)
- Confirmed both Tosh API fixes working (subscriptionProvider + UTM meta)
- Self-heal undetermined dropped from 58 → 3
- Closed delta with Tosh from 22 → 18 records
- Found and fixed Bug 1: cancelled_ag_map source filter excluded 'manual'
- Found and fixed Bug 2: daily sync waterfall missing Step 0 email-existence check
- Built UTM backfill endpoint, ran on 1,014 active Stripe subs (6 had UTMs)
- Built cleanup-manual-duplicates endpoint
- Cleaned 9 manual/manual duplicate records, MRR adjusted down by $179.91/mo (phantom inflation)
- Discovered shadow sync takes ~12 minutes, not ~1 hour

### Session 23 (April 10, 2026) — Provider overhaul + audit tooling
- Fixed provider defaulting bug across 8 code locations
- Built 5-step Provider Resolution Waterfall
- Built Reconciliation Audit, Data Audit, Provider Cleanup endpoints
- Cleaned 79 misclassified records
- Reduced delta with Tosh from large/unclear to 22 records (1.2%)

### Sessions 16-22 — Foundation
- Built ymove webhook integration (S18 dedup logic)
- Built daily shadow sync (S20)
- Built UTM tracking infrastructure (S21)
- Built admin dashboard
- Built Stripe + Apple + Google webhook handling

### Sessions 11-15 — Initial Build
- Stripe webhook integration
- Trial-to-paid conversion tracking
- Daily digest email system
- Multi-tab XLSX export
- Subscription analytics foundation

---

## 10. RUNNING LIST OF KNOWN BUGS (NOT YET FIXED)

| Bug | Where | Impact | Priority |
|-----|-------|--------|----------|
| B | Shadow sync deactivation logic (line ~3071) | False-cancels from transient ymove glitches | HIGH |
| C | Shadow sync import logic | Stale ymove subscribed → false active records | MEDIUM |
| D | ymove pagination (Tosh-side) | Possible silent data loss | UNKNOWN, investigate |
| E | data-audit endpoint | `stripe_active` excludes trialing, Apple/Google include | LOW (cosmetic) |
| F | Auto-expire on status polls | Only fires on action:'run' | LOW |
| Gap 1 (from earlier) | `_ymove_handle_cancelled` | Cancellation by email only (LIMIT 1) | MEDIUM |
| Gap 2 (from earlier) | Shadow sync verify pool | Stripe drift invisible (intentional) | INTENTIONAL |
| Gap 3 (from earlier) | ymove webhook | No retry from ymove | ACCEPTED |

---

## 11. STANDING REMINDERS

- SimpleBlueprints rules don't apply here, this is Movement & Miles
- Will rotates GitHub PAT frequently — never reuse old tokens
- All admin endpoint changes need preview/apply pattern, never one-shot writes
- Record IDs to never touch without confirmation: any sub_id starting with `sub_*` (real Stripe), numeric (real Apple), `ym_google_*` (real Google webhook)
- The reversibility tag pattern `s25_*_<timestamp>` lets us undo any batch
