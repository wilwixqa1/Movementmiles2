from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
import httpx
import os
import json
import asyncpg
import stripe
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import gzip
import asyncio

app = FastAPI(title="Movement & Miles")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Config ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "mmadmin2026")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
DIGEST_RECIPIENTS = os.environ.get("DIGEST_RECIPIENTS", "")
DIGEST_FROM_EMAIL = os.environ.get("DIGEST_FROM_EMAIL", "onboarding@resend.dev")

stripe.api_key = STRIPE_SECRET_KEY

# Apple App Store Connect API
APPLE_KEY_ID = os.environ.get("APPLE_KEY_ID", "")
APPLE_ISSUER_ID = os.environ.get("APPLE_ISSUER_ID", "")
APPLE_KEY_CONTENT = os.environ.get("APPLE_KEY_CONTENT", "")
APPLE_VENDOR_NUMBER = os.environ.get("APPLE_VENDOR_NUMBER", "")

# ymove API (Session 17)
YMOVE_API_KEY = os.environ.get("YMOVE_API_KEY", "")
YMOVE_API_BASE = "https://v6-beta-api.ymove.app"
YMOVE_SITE_ID = "75"  # Movement & Miles site ID (from Tosh, S19)

# --- Database ---
db_pool = None

# --- Scheduler ---
scheduler = None

@app.on_event("startup")
async def startup():
    global db_pool, scheduler
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

            # Block 1: Core tables (leads, page_views, chat_sessions)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id SERIAL PRIMARY KEY,
                        first_name TEXT,
                        email TEXT,
                        experience_level TEXT,
                        goals TEXT,
                        referral_source TEXT,
                        recommended_plan TEXT,
                        extra TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS page_views (
                        id SERIAL PRIMARY KEY,
                        page TEXT,
                        path TEXT,
                        referrer TEXT,
                        user_agent TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS chat_sessions (
                        id SERIAL PRIMARY KEY,
                        session_type TEXT,
                        message_count INTEGER DEFAULT 1,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                # UTM tracking columns on leads
                for col in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'ym_source']:
                    await conn.execute(f"ALTER TABLE leads ADD COLUMN IF NOT EXISTS {col} TEXT DEFAULT ''")
            print("[Startup] Block 1: Core tables ready")

            # Block 2: Subscription tables + ALTER columns
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        id SERIAL PRIMARY KEY,
                        stripe_customer_id TEXT,
                        stripe_subscription_id TEXT UNIQUE,
                        email TEXT,
                        status TEXT,
                        plan_interval TEXT,
                        plan_amount INTEGER,
                        currency TEXT DEFAULT 'usd',
                        source TEXT DEFAULT 'stripe',
                        trial_start TIMESTAMPTZ,
                        trial_end TIMESTAMPTZ,
                        current_period_start TIMESTAMPTZ,
                        current_period_end TIMESTAMPTZ,
                        canceled_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'stripe'")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS readable_id TEXT")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS renewal_count INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS last_renewed_at TIMESTAMPTZ")
                # S16: import batch tracking for safe revert
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS import_batch TEXT")
                # S17: store names directly on subscriptions
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS first_name TEXT")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS last_name TEXT")
                # S21: UTM attribution from ymove meta parameters
                for col in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'ym_source']:
                    await conn.execute(f"ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS {col} TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS utm_meta_raw JSONB")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscription_events (
                        id SERIAL PRIMARY KEY,
                        stripe_event_id TEXT UNIQUE,
                        event_type TEXT,
                        stripe_customer_id TEXT,
                        stripe_subscription_id TEXT,
                        source TEXT DEFAULT 'stripe',
                        data JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("ALTER TABLE subscription_events ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'stripe'")
            print("[Startup] Block 2: Subscription tables ready")

            # Block 3: Analytics tables (ad_spend, platform_metrics, ymove_webhook_log)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ad_spend (
                        id SERIAL PRIMARY KEY,
                        month TEXT NOT NULL,
                        channel TEXT NOT NULL,
                        amount_cents INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(month, channel)
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS platform_metrics (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL,
                        source TEXT NOT NULL,
                        metric_type TEXT NOT NULL,
                        active_subscriptions INTEGER DEFAULT 0,
                        active_free_trials INTEGER DEFAULT 0,
                        new_subscriptions INTEGER DEFAULT 0,
                        renewals INTEGER DEFAULT 0,
                        conversions INTEGER DEFAULT 0,
                        cancellations INTEGER DEFAULT 0,
                        reactivations INTEGER DEFAULT 0,
                        revenue_cents INTEGER DEFAULT 0,
                        proceeds_cents INTEGER DEFAULT 0,
                        report_data JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(date, source, metric_type)
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ymove_webhook_log (
                        id SERIAL PRIMARY KEY,
                        event_type TEXT,
                        payload JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ymove_sync_runs (
                        id SERIAL PRIMARY KEY,
                        status TEXT DEFAULT 'running',
                        started_at TIMESTAMPTZ DEFAULT NOW(),
                        completed_at TIMESTAMPTZ,
                        phase TEXT DEFAULT 'init',
                        progress_current INTEGER DEFAULT 0,
                        progress_total INTEGER DEFAULT 0,
                        our_active_count INTEGER DEFAULT 0,
                        ymove_active_count INTEGER DEFAULT 0,
                        results JSONB,
                        error TEXT
                    )
                """)
            print("[Startup] Block 3: Analytics tables ready")

            # Block 4: Indexes (non-blocking, failure will not kill startup)
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_email_lower ON leads (lower(email))")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_subs_email_lower ON subscriptions (lower(email))")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_subs_trial_start ON subscriptions (trial_start) WHERE trial_start IS NOT NULL")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_subs_import_batch ON subscriptions (import_batch) WHERE import_batch IS NOT NULL")
                print("[Startup] Block 4: Indexes ready")
            except Exception as e:
                print(f"[Startup] Index creation deferred (non-fatal): {e}")
        except Exception as e:
            print(f"Database connection failed: {e}")
            db_pool = None
    else:
        print("No DATABASE_URL â running without database")

    # Start daily digest scheduler
    if RESEND_API_KEY and DIGEST_RECIPIENTS:
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
            from apscheduler.triggers.cron import CronTrigger
            scheduler = AsyncIOScheduler()
            et = ZoneInfo("America/New_York")
            scheduler.add_job(
                run_daily_digest,
                CronTrigger(hour=9, minute=0, timezone=et),
                id="daily_digest",
                replace_existing=True,
            )
            # S20: Daily shadow sync at 8:00 AM ET (1 hour before digest)
            if YMOVE_API_KEY:
                scheduler.add_job(
                    run_daily_shadow_sync,
                    CronTrigger(hour=8, minute=0, timezone=et),
                    id="daily_shadow_sync",
                    replace_existing=True,
                )
            scheduler.start()
            ymove_sync_msg = " + shadow sync 8:00 AM ET" if YMOVE_API_KEY else ""
            print(f"Daily digest scheduler started (9:00 AM ET -> {DIGEST_RECIPIENTS}{ymove_sync_msg})")
        except Exception as e:
            print(f"Scheduler startup error: {e}")
    else:
        missing = []
        if not RESEND_API_KEY:
            missing.append("RESEND_API_KEY")
        if not DIGEST_RECIPIENTS:
            missing.append("DIGEST_RECIPIENTS")
        print(f"Daily digest disabled â missing: {', '.join(missing)}")

@app.on_event("shutdown")
async def shutdown():
    global db_pool, scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
    if db_pool:
        await db_pool.close()


# --- System Prompts ---

NELLY_SYSTEM_PROMPT = """You are Nelly, the AI coaching assistant for Movement & Miles (M&M), a holistic running and fitness app created by coach Meg.

PERSONALITY: Warm, encouraging, conversational. You talk like a friend who happens to be a running coach. Keep responses SHORT (2-4 sentences max). Never dump walls of text.

CRITICAL CONVERSATION RULE: Ask ONE question at a time. Never list multiple questions. Have a natural back-and-forth conversation. Guide them step by step.

LINK FORMAT: When recommending a program or answering about a page, include a clickable link using this format:
[[page:PageName]]
Available pages: [[page:Training Programs]], [[page:Race Plans]], [[page:Store]]

BUTTON FORMAT: When you want to give the user options to choose from, end your message with options in this exact format on a new line:
[Option A | Option B | Option C]

ONLY use options when there are clear choices. For open-ended questions, just ask normally without options.

CRITICAL: You may ONLY recommend programs that exist in the app. NEVER invent program names.

COMPLETE PROGRAM LIST:

RUNNING + STRENGTH MONTHLY PLANS:
Beginner: Walk to Run Part 1, Walk to Run Part 2, Miles + Bodyweight Strength, Building Endurance & Strength, Beginners: Total Package
Intermediate: Strides + Calisthenics, Outdoor Miles + Weights, Balanced Strides & Strength, Endurance & Strength
Advanced: Run + Lift, Endurance Speed & Strength, Peak Endurance & Power, 7 Weeks to 10 Miles

STRENGTH-ONLY PLANS:
Beginner: Bodyweight & Bands (4wk), Strength Starts Here (2wk), Pure Strength (6wk)
Intermediate: Stronger Strides (6wk), Total Body Power (6wk), Well Built
Advanced: Total Power & Strength (6wk), Cross-Training Power

MOBILITY & PREHAB:
All Levels: Prehab: Knee/ITBS + Mobility (4wk), Mobility Master (4wk), Trail/Road Running Prehab Essentials (6wk), Ankle Foot and Calf Strength (10 modules), ITBS/Knee Pain Workout Collection (10 modules), The Ultimate Mobility Plan (4wk)

RACE PLANS:
Beginner: 8-Week Beginner 5K Treadmill & Outdoor, Beginner 5K Plan (Outdoor), Beginner 10K Plan (Tread & Outdoor), Beginner 10K Plan (Outdoor), Beginner Half Marathon Plan (20wk), Beginner Marathon Plan (20wk)
Intermediate: Intermediate 5K Plan (Tread & Outdoor), Intermediate 5K Plan (Outdoor), Intermediate 10K Plan (Tread & Outdoor), Intermediate 10K Plan (Outdoor), Intermediate Half Marathon Plan (10wk), Intermediate 16-Week Marathon Plan
Advanced: Advanced Half Marathon (12wk), Advanced Marathon Plan, 50K Race Plan (16wk)

DETRAINING PLANS:
Beginner: Detrain Protocol
Intermediate: Recover, Restore & Reset
Advanced: The Adaptation Block

NUTRITION PLANS: Endurance Nutrition, Strength Nutrition, Weight Loss Nutrition

PROGRESSIONS:
BEGINNER RUNNING: Walk to Run Part 1 > Walk to Run Part 2 > Miles + Bodyweight Strength > Building Endurance & Strength > Beginners: Total Package
INTERMEDIATE RUNNING: Strides + Calisthenics > Outdoor Miles + Weights > Balanced Strides & Strength > Endurance & Strength
ADVANCED RUNNING: Run + Lift > Endurance Speed & Strength > Peak Endurance & Power > 7 Weeks to 10 Miles
BEGINNER STRENGTH: Bodyweight & Bands > Strength Starts Here > Pure Strength
INTERMEDIATE STRENGTH: Stronger Strides > Total Body Power > Well Built
ADVANCED STRENGTH: Total Power & Strength > Cross-Training Power
RACE ORDER: 5K > 10K > Half Marathon > Marathon > 50K (never skip)

DETRAINING RULES:
- After running/race program AND person has NOT taken 3+ weeks off: recommend detraining first
- After running/race program AND person HAS taken 3+ weeks off: skip detraining
- After strength-only program: NO detraining needed

EQUIPMENT: Ask about weights (kettlebells or dumbbells; add barbell for advanced). Ask about treadmill preference.

CONVERSATION FLOW for plan recommendations:
Set expectations first, then ask ONE AT A TIME:
1. Running+strength, strength only, or train for a race?
2. What level?
3. Can you run 3 miles without stopping?
4. Any pain?
5. Access to weights?
6. Treadmill preference?
7. If race: when is it and what distance?
Max 7 questions, then give 3 OPTIONS.

FAQs:
CANCEL: Apple/Google > subscription settings. Website > app profile > Info > Manage Subscription
PRICING: Monthly $19.99, Annual $179.99
INCLUDED: Everything - all programs, plans, nutrition
GARMIN/ANNUAL SWITCH: email support@movementandmiles.com
PAYMENT: https://movementandmiles.ymove.app/account
MISSED WORKOUTS: 1-2 = continue. 3-5 = resume easier. Week+ = repeat previous week.

FINAL RECOMMENDATIONS: Always present exactly 3 options with one-sentence explanations. Use button format."""


DIGEST_SYSTEM_PROMPT = """You are an analytics advisor for Movement & Miles, a fitness subscription app. You receive daily metrics and generate a brief, actionable morning digest for the business owner.

CRITICAL CONTEXT â TRIAL vs PAID:
- "new_subscriptions_trial_starts" are TRIAL STARTS â these are $0 revenue. The checkout flow gives 1 month free, then $19.99/month.
- "conversions_today" are the real revenue events â people whose free trial ended and converted to paid.
- Do NOT celebrate new subscriptions as revenue. Frame them as "pipeline" or "trial starts."
- Conversions are what matter for revenue. Highlight them prominently when they occur.
- A healthy business needs both: new trials (top of funnel) AND conversions (actual revenue).

RULES:
- Be concise: 3-5 bullet points max for insights
- Lead with conversions and revenue, then trial starts as pipeline
- Compare to context when possible (e.g. "above/below your typical daily rate")
- Flag anything unusual or concerning
- Suggest ONE specific action if data warrants it
- Use plain language, not jargon
- If a day had zero activity in some area, just note it briefly, don't over-analyze
- Platform fee context: Apple and Google take 15%, Stripe takes ~2.9%

FORMAT your response as a short paragraph overview, then bullet points for key insights. Keep total response under 200 words."""


# --- Shared Helpers ---

async def call_anthropic(system_prompt: str, messages: list, max_tokens: int = 800, cache_system: bool = False) -> str:
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="API key not configured")
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    if cache_system:
        system_val = [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}]
        headers["anthropic-beta"] = "prompt-caching-2024-07-31"
    else:
        system_val = system_prompt
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_val,
                    "messages": messages,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail=f"Anthropic API error: {e.response.status_code}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


async def call_anthropic_raw(system_prompt: str, messages: list, max_tokens: int = 800, cache_system: bool = False) -> str:
    """Like call_anthropic but doesn't raise HTTPException â returns error string instead."""
    if not ANTHROPIC_API_KEY:
        return "[Error: ANTHROPIC_API_KEY not configured]"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    if cache_system:
        system_val = [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}]
        headers["anthropic-beta"] = "prompt-caching-2024-07-31"
    else:
        system_val = system_prompt
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_val,
                    "messages": messages,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except httpx.HTTPStatusError as e:
            body = e.response.text if hasattr(e.response, 'text') else 'no body'
            print(f"[Anthropic API error] Status: {e.response.status_code}, Body: {body}")
            return f"[AI insights unavailable: {e.response.status_code} - {body[:200]}]"
        except Exception as e:
            print(f"[Anthropic API error] {type(e).__name__}: {str(e)}")
            return f"[AI insights unavailable: {str(e)}]"


def require_admin(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


# --- Readable ID Assignment ---

async def assign_readable_id(conn, source: str) -> str:
    """Generate next persistent readable ID for a source (APPLE-0001, GOOGLE-0001, STRIPE-0001)."""
    prefix = source.upper()  # APPLE, GOOGLE, STRIPE
    row = await conn.fetchrow(
        "SELECT readable_id FROM subscriptions WHERE source = $1 AND readable_id IS NOT NULL ORDER BY readable_id DESC LIMIT 1",
        source
    )
    if row and row["readable_id"]:
        # Extract number from e.g. APPLE-0042
        try:
            num = int(row["readable_id"].split("-")[-1]) + 1
        except (ValueError, IndexError):
            num = 1
    else:
        num = 1
    return f"{prefix}-{num:04d}"


# --- Pydantic Models ---

class ChatRequest(BaseModel):
    message: str
    history: list = []
    source: str = "widget"
    source: str = "widget"
    source: str = "widget"

class ChatResponse(BaseModel):
    reply: str

class LeadRequest(BaseModel):
    first_name: str = ""
    email: str = ""
    experience_level: str = ""
    goals: str = ""
    referral_source: str = ""
    recommended_plan: str = ""
    extra: str = ""
    utm_source: str = ""
    utm_medium: str = ""
    utm_campaign: str = ""
    utm_term: str = ""
    utm_content: str = ""
    ym_source: str = ""

class PageViewRequest(BaseModel):
    page: str = ""
    path: str = ""
    referrer: str = ""

class LoginRequest(BaseModel):
    password: str

class AdSpendRequest(BaseModel):
    month: str = ""
    channel: str = ""
    amount_dollars: float = 0.0


# --- API Routes ---

# Chat endpoints
@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    messages = []
    for msg in req.history[-20:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})
    reply = await call_anthropic(NELLY_SYSTEM_PROMPT, messages, cache_system=True)
    # Track chat session
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO chat_sessions (session_type, message_count) VALUES ($1, $2)",
                    req.source or "widget", 1
                )
        except Exception:
            pass
    return ChatResponse(reply=reply)


@app.post("/api/lead")
async def save_lead(lead: LeadRequest):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO leads (first_name, email, experience_level, goals, referral_source, recommended_plan, extra, utm_source, utm_medium, utm_campaign, utm_term, utm_content, ym_source)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                    lead.first_name, lead.email, lead.experience_level,
                    lead.goals, lead.referral_source, lead.recommended_plan, lead.extra,
                    lead.utm_source, lead.utm_medium, lead.utm_campaign,
                    lead.utm_term, lead.utm_content, lead.ym_source
                )
            return {"status": "saved", "storage": "postgres"}
        except Exception as e:
            print(f"DB lead save error: {e}")
    # Fallback to JSON
    try:
        leads = []
        if os.path.exists("leads.json"):
            with open("leads.json", "r") as f:
                leads = json.load(f)
        leads.append(lead.dict())
        with open("leads.json", "w") as f:
            json.dump(leads, f, indent=2)
        return {"status": "saved", "storage": "json"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Page view tracking
@app.post("/api/page-view")
async def track_page_view(pv: PageViewRequest, request: Request):
    if db_pool:
        try:
            ua = request.headers.get("user-agent", "")
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO page_views (page, path, referrer, user_agent) VALUES ($1, $2, $3, $4)",
                    pv.page, pv.path, pv.referrer, ua
                )
        except Exception as e:
            print(f"Page view error: {e}")
    return {"status": "ok"}


# --- Stripe Webhook ---

@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    # Verify webhook signature if secret is configured
    if STRIPE_WEBHOOK_SECRET:
        try:
            event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        except stripe.error.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Invalid signature")
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    else:
        # No webhook secret configured â parse raw (dev/testing only)
        try:
            event = json.loads(payload)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid payload")

    event_type = event.get("type", "")
    event_id = event.get("id", "")
    data_obj = event.get("data", {}).get("object", {})

    if not db_pool:
        return {"status": "ok", "note": "no database"}

    async with db_pool.acquire() as conn:
        # Store raw event (idempotent via unique stripe_event_id)
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_customer_id, stripe_subscription_id, data)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                event_id,
                event_type,
                data_obj.get("customer", ""),
                data_obj.get("id", "") if "sub_" in data_obj.get("id", "") else data_obj.get("subscription", ""),
                json.dumps(data_obj)
            )
        except Exception as e:
            print(f"Event store error: {e}")

        # Handle subscription events
        if event_type in (
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.deleted",
            "customer.subscription.trial_will_end",
        ):
            sub = data_obj
            sub_id = sub.get("id", "")
            customer_id = sub.get("customer", "")
            status = sub.get("status", "")

            # Extract plan info from items
            plan_amount = 0
            plan_interval = ""
            items = sub.get("items", {}).get("data", [])
            if items:
                price = items[0].get("price", {}) or items[0].get("plan", {})
                plan_amount = price.get("unit_amount", 0)
                plan_interval = price.get("recurring", {}).get("interval", "") if price.get("recurring") else price.get("interval", "")

            # Get customer email
            email = ""
            if customer_id and STRIPE_SECRET_KEY:
                try:
                    cust = stripe.Customer.retrieve(customer_id)
                    email = cust.get("email", "")
                except Exception:
                    pass

            # Convert timestamps
            def ts(v):
                if v:
                    return datetime.fromtimestamp(v, tz=timezone.utc)
                return None

            try:
                await conn.execute("""
                    INSERT INTO subscriptions (
                        stripe_customer_id, stripe_subscription_id, email, status,
                        plan_interval, plan_amount, currency, source,
                        trial_start, trial_end,
                        current_period_start, current_period_end,
                        canceled_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,'stripe',$8,$9,$10,$11,$12,NOW())
                    ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        plan_interval = EXCLUDED.plan_interval,
                        plan_amount = EXCLUDED.plan_amount,
                        email = EXCLUDED.email,
                        trial_start = EXCLUDED.trial_start,
                        trial_end = EXCLUDED.trial_end,
                        current_period_start = EXCLUDED.current_period_start,
                        current_period_end = EXCLUDED.current_period_end,
                        canceled_at = EXCLUDED.canceled_at,
                        updated_at = NOW()
                """,
                    customer_id, sub_id, email, status,
                    plan_interval, plan_amount, sub.get("currency", "usd"),
                    ts(sub.get("trial_start")), ts(sub.get("trial_end")),
                    ts(sub.get("current_period_start")), ts(sub.get("current_period_end")),
                    ts(sub.get("canceled_at"))
                )
            except Exception as e:
                print(f"Subscription upsert error: {e}")

            # Session 11: Assign readable_id if not yet set
            try:
                existing = await conn.fetchrow(
                    "SELECT readable_id FROM subscriptions WHERE stripe_subscription_id = $1",
                    sub_id
                )
                if existing and not existing["readable_id"]:
                    rid = await assign_readable_id(conn, "stripe")
                    await conn.execute(
                        "UPDATE subscriptions SET readable_id = $1 WHERE stripe_subscription_id = $2",
                        rid, sub_id
                    )
            except Exception as e:
                print(f"Readable ID assign error: {e}")

            # Session 11: Track renewals (invoice.paid-like events)
            if event_type == "customer.subscription.updated" and status == "active":
                prev_attrs = event.get("data", {}).get("previous_attributes", {})
                prev_period = prev_attrs.get("current_period_start")
                if prev_period and prev_period != sub.get("current_period_start"):
                    try:
                        await conn.execute(
                            """UPDATE subscriptions SET
                               renewal_count = COALESCE(renewal_count, 0) + 1,
                               last_renewed_at = NOW()
                               WHERE stripe_subscription_id = $1""",
                            sub_id
                        )
                    except Exception:
                        pass

            # Session 11: Detect trial-to-paid conversion (trialing -> active)
            if event_type == "customer.subscription.updated":
                prev_attrs = event.get("data", {}).get("previous_attributes", {})
                if prev_attrs.get("status") == "trialing" and status == "active":
                    try:
                        await conn.execute(
                            "UPDATE subscriptions SET converted_at = NOW() WHERE stripe_subscription_id = $1 AND converted_at IS NULL",
                            sub_id
                        )
                        print(f"Trial conversion stamped for {sub_id}")
                    except Exception as e:
                        print(f"Conversion stamp error: {e}")

        # Handle one-time payments (checkout.session.completed for initial signup)
        elif event_type == "checkout.session.completed":
            session = data_obj
            # If this checkout created a subscription, it'll be handled by subscription.created
            # Log it for analytics
            print(f"Checkout completed: {session.get('id')}, customer: {session.get('customer')}")

    return {"status": "ok"}


# --- Apple App Store Server Notifications v2 ---

@app.post("/webhooks/apple")
async def apple_webhook(request: Request):
    """
    Apple sends JWS (JSON Web Signature) signed payloads.
    We decode the payload to extract notification type and transaction info.
    Full JWS signature verification can be added later with PyJWT + Apple root certs.
    """
    import base64

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    signed_payload = body.get("signedPayload", "")
    if not signed_payload:
        raise HTTPException(status_code=400, detail="Missing signedPayload")

    # Decode JWS payload (header.payload.signature â we want the middle part)
    try:
        parts = signed_payload.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWS format")
        # Base64url decode the payload
        payload_b64 = parts[1]
        # Add padding if needed
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        notification = json.loads(payload_bytes)
    except Exception as e:
        print(f"Apple JWS decode error: {e}")
        raise HTTPException(status_code=400, detail="Failed to decode payload")

    notification_type = notification.get("notificationType", "")
    subtype = notification.get("subtype", "")

    # Decode the signed transaction info
    transaction_info = {}
    signed_transaction = notification.get("data", {}).get("signedTransactionInfo", "")
    if signed_transaction:
        try:
            t_parts = signed_transaction.split(".")
            if len(t_parts) == 3:
                t_b64 = t_parts[1]
                t_padding = 4 - len(t_b64) % 4
                if t_padding != 4:
                    t_b64 += "=" * t_padding
                transaction_info = json.loads(base64.urlsafe_b64decode(t_b64))
        except Exception as e:
            print(f"Apple transaction decode error: {e}")

    # Decode renewal info
    renewal_info = {}
    signed_renewal = notification.get("data", {}).get("signedRenewalInfo", "")
    if signed_renewal:
        try:
            r_parts = signed_renewal.split(".")
            if len(r_parts) == 3:
                r_b64 = r_parts[1]
                r_padding = 4 - len(r_b64) % 4
                if r_padding != 4:
                    r_b64 += "=" * r_padding
                renewal_info = json.loads(base64.urlsafe_b64decode(r_b64))
        except Exception as e:
            print(f"Apple renewal decode error: {e}")

    original_transaction_id = transaction_info.get("originalTransactionId", "")
    product_id = transaction_info.get("productId", "")

    # Map Apple notification types to our status
    status_map = {
        "SUBSCRIBED": "active",
        "DID_RENEW": "active",
        "DID_CHANGE_RENEWAL_STATUS": "active",  # could be turning off auto-renew
        "EXPIRED": "canceled",
        "DID_FAIL_TO_RENEW": "past_due",
        "GRACE_PERIOD_EXPIRED": "canceled",
        "REFUND": "canceled",
        "REVOKE": "canceled",
        "CONSUMPTION_REQUEST": "active",
    }
    status = status_map.get(notification_type, "active")
    if notification_type == "DID_CHANGE_RENEWAL_STATUS" and subtype == "AUTO_RENEW_DISABLED":
        status = "canceled"

    # Determine plan from product ID
    plan_interval = "month"
    plan_amount = 1999  # $19.99 default
    if product_id:
        pid_lower = product_id.lower()
        if "annual" in pid_lower or "year" in pid_lower:
            plan_interval = "year"
            plan_amount = 17999  # $179.99

    if not db_pool or not original_transaction_id:
        return {"status": "ok"}

    async with db_pool.acquire() as conn:
        # Store event
        event_id = f"apple_{notification_type}_{original_transaction_id}_{int(datetime.now(timezone.utc).timestamp())}"
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_customer_id, stripe_subscription_id, source, data)
                   VALUES ($1, $2, $3, $4, 'apple', $5)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                event_id,
                f"apple.{notification_type}",
                "",
                original_transaction_id,
                json.dumps({"notification": notification_type, "subtype": subtype, "product_id": product_id, "transaction": transaction_info})
            )
        except Exception as e:
            print(f"Apple event store error: {e}")

        # Upsert subscription
        try:
            expires_ms = transaction_info.get("expiresDate", 0)
            purchase_ms = transaction_info.get("purchaseDate", 0)

            def ms_to_dt(ms):
                if ms:
                    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                return None

            await conn.execute("""
                INSERT INTO subscriptions (
                    stripe_customer_id, stripe_subscription_id, email, status,
                    plan_interval, plan_amount, currency, source,
                    current_period_start, current_period_end,
                    canceled_at, updated_at
                ) VALUES ('', $1, '', $2, $3, $4, 'usd', 'apple', $5, $6, $7, NOW())
                ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    plan_interval = EXCLUDED.plan_interval,
                    plan_amount = EXCLUDED.plan_amount,
                    current_period_start = EXCLUDED.current_period_start,
                    current_period_end = EXCLUDED.current_period_end,
                    canceled_at = EXCLUDED.canceled_at,
                    updated_at = NOW()
            """,
                original_transaction_id, status,
                plan_interval, plan_amount,
                ms_to_dt(purchase_ms), ms_to_dt(expires_ms),
                ms_to_dt(expires_ms) if status == "canceled" else None
            )
        except Exception as e:
            print(f"Apple subscription upsert error: {e}")

        # Session 11: Assign readable_id if not yet set
        try:
            existing = await conn.fetchrow(
                "SELECT readable_id FROM subscriptions WHERE stripe_subscription_id = $1",
                original_transaction_id
            )
            if existing and not existing["readable_id"]:
                rid = await assign_readable_id(conn, "apple")
                await conn.execute(
                    "UPDATE subscriptions SET readable_id = $1 WHERE stripe_subscription_id = $2",
                    rid, original_transaction_id
                )
        except Exception as e:
            print(f"Apple readable ID error: {e}")

        # Session 11: Track renewals on DID_RENEW
        if notification_type == "DID_RENEW":
            try:
                await conn.execute(
                    """UPDATE subscriptions SET
                       renewal_count = COALESCE(renewal_count, 0) + 1,
                       last_renewed_at = NOW()
                       WHERE stripe_subscription_id = $1""",
                    original_transaction_id
                )
            except Exception:
                pass

        # Apple trial-to-paid: DID_RENEW after SUBSCRIBED = converted
        if notification_type == "DID_RENEW":
            try:
                await conn.execute(
                    """UPDATE subscriptions SET converted_at = NOW()
                       WHERE stripe_subscription_id = $1 AND converted_at IS NULL
                       AND trial_start IS NOT NULL""",
                    original_transaction_id
                )
            except Exception:
                pass

    return {"status": "ok"}


# --- Google Play Real-Time Developer Notifications ---

@app.post("/webhooks/google")
async def google_webhook(request: Request):
    """
    Google sends RTDN via Cloud Pub/Sub push subscription.
    The payload contains a base64-encoded subscription notification.
    Full verification via Google Play Developer API can be added later.
    """
    import base64

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Google Pub/Sub wraps the message
    message = body.get("message", {})
    data_b64 = message.get("data", "")

    if not data_b64:
        # Might be a direct notification format
        data_b64 = body.get("data", "")

    if not data_b64:
        return {"status": "ok", "note": "no data"}

    # Decode the notification
    try:
        padding = 4 - len(data_b64) % 4
        if padding != 4:
            data_b64 += "=" * padding
        notification = json.loads(base64.b64decode(data_b64))
    except Exception as e:
        print(f"Google RTDN decode error: {e}")
        raise HTTPException(status_code=400, detail="Failed to decode notification")

    package_name = notification.get("packageName", "")
    sub_notification = notification.get("subscriptionNotification", {})

    if not sub_notification:
        # Might be a one-time purchase or test notification
        print(f"Google non-subscription notification: {notification}")
        return {"status": "ok"}

    notification_type = sub_notification.get("notificationType", 0)
    purchase_token = sub_notification.get("purchaseToken", "")
    subscription_id = sub_notification.get("subscriptionId", "")

    # Map Google notification types to our status
    # https://developer.android.com/google/play/billing/rtdn-reference
    google_type_map = {
        1: ("google.RECOVERED", "active"),           # SUBSCRIPTION_RECOVERED
        2: ("google.RENEWED", "active"),              # SUBSCRIPTION_RENEWED
        3: ("google.CANCELED", "canceled"),            # SUBSCRIPTION_CANCELED
        4: ("google.PURCHASED", "active"),             # SUBSCRIPTION_PURCHASED
        5: ("google.ON_HOLD", "past_due"),             # SUBSCRIPTION_ON_HOLD
        6: ("google.IN_GRACE_PERIOD", "past_due"),     # SUBSCRIPTION_IN_GRACE_PERIOD
        7: ("google.RESTARTED", "active"),             # SUBSCRIPTION_RESTARTED
        8: ("google.PRICE_CHANGE_CONFIRMED", "active"),# SUBSCRIPTION_PRICE_CHANGE_CONFIRMED
        9: ("google.DEFERRED", "active"),              # SUBSCRIPTION_DEFERRED
        10: ("google.PAUSED", "canceled"),             # SUBSCRIPTION_PAUSED
        11: ("google.PAUSE_SCHEDULE_CHANGED", "active"),
        12: ("google.REVOKED", "canceled"),            # SUBSCRIPTION_REVOKED
        13: ("google.EXPIRED", "canceled"),            # SUBSCRIPTION_EXPIRED
        20: ("google.PENDING_PURCHASE_CANCELED", "canceled"),
    }

    event_name, status = google_type_map.get(notification_type, (f"google.UNKNOWN_{notification_type}", "active"))

    # Determine plan from subscription ID
    plan_interval = "month"
    plan_amount = 1999
    if subscription_id:
        sid_lower = subscription_id.lower()
        if "annual" in sid_lower or "year" in sid_lower:
            plan_interval = "year"
            plan_amount = 17999

    # Use purchase_token as the unique ID (truncate if very long)
    external_id = f"gp_{purchase_token[:80]}" if purchase_token else ""

    if not db_pool or not external_id:
        return {"status": "ok"}

    async with db_pool.acquire() as conn:
        # Store event
        event_id = f"google_{notification_type}_{purchase_token[:40]}_{int(datetime.now(timezone.utc).timestamp())}"
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_customer_id, stripe_subscription_id, source, data)
                   VALUES ($1, $2, $3, $4, 'google', $5)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                event_id,
                event_name,
                "",
                external_id,
                json.dumps({"notification_type": notification_type, "subscription_id": subscription_id, "package": package_name})
            )
        except Exception as e:
            print(f"Google event store error: {e}")

        # Upsert subscription
        try:
            await conn.execute("""
                INSERT INTO subscriptions (
                    stripe_customer_id, stripe_subscription_id, email, status,
                    plan_interval, plan_amount, currency, source,
                    updated_at
                ) VALUES ('', $1, '', $2, $3, $4, 'usd', 'google', NOW())
                ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    plan_interval = EXCLUDED.plan_interval,
                    plan_amount = EXCLUDED.plan_amount,
                    canceled_at = CASE WHEN EXCLUDED.status = 'canceled' THEN NOW() ELSE subscriptions.canceled_at END,
                    updated_at = NOW()
            """,
                external_id, status,
                plan_interval, plan_amount
            )
        except Exception as e:
            print(f"Google subscription upsert error: {e}")

        # Session 11: Assign readable_id if not yet set
        try:
            existing = await conn.fetchrow(
                "SELECT readable_id FROM subscriptions WHERE stripe_subscription_id = $1",
                external_id
            )
            if existing and not existing["readable_id"]:
                rid = await assign_readable_id(conn, "google")
                await conn.execute(
                    "UPDATE subscriptions SET readable_id = $1 WHERE stripe_subscription_id = $2",
                    rid, external_id
                )
        except Exception as e:
            print(f"Google readable ID error: {e}")

        # Session 11: Track renewals on RENEWED (2) or RECOVERED (1)
        if notification_type in (1, 2):
            try:
                await conn.execute(
                    """UPDATE subscriptions SET
                       renewal_count = COALESCE(renewal_count, 0) + 1,
                       last_renewed_at = NOW()
                       WHERE stripe_subscription_id = $1""",
                    external_id
                )
            except Exception:
                pass

        # Session 11: Google trial-to-paid conversion
        # RENEWED (type 2) or RECOVERED (type 1) after initial purchase = likely converted
        if notification_type in (1, 2):
            try:
                await conn.execute(
                    """UPDATE subscriptions SET converted_at = NOW()
                       WHERE stripe_subscription_id = $1 AND converted_at IS NULL
                       AND trial_start IS NOT NULL""",
                    external_id
                )
            except Exception:
                pass

    return {"status": "ok"}


# --- ymove Webhook (Session 16 â Phase 2 Processor) ---

@app.post("/webhooks/ymove")
async def ymove_webhook(request: Request):
    """Receive subscription events from ymove Actions system.
    Phase 2: Process into subscriptions table with email attribution."""
    try:
        body = await request.json()
    except Exception:
        body = {"raw": (await request.body()).decode("utf-8", errors="replace")[:5000]}

    # Extract event data
    event_data = body.get("event", {})
    user_data = body.get("user", {})

    event_type = event_data.get("type", "unknown")
    category = event_data.get("category", "")

    # User info â available on ALL events (key advantage over direct Apple/Google webhooks)
    email = user_data.get("email", "")
    first_name = user_data.get("firstName", "")
    last_name = user_data.get("lastName", "")
    ymove_user_id = str(user_data.get("id", ""))

    provider = event_data.get("subscriptionPaymentProvider", "")
    print(f"[ymove] {event_type} | {email} | provider={provider or 'n/a'}")

    # Always log raw payload
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ymove_webhook_log (event_type, payload) VALUES ($1, $2)",
                    event_type, json.dumps(body, default=str)
                )
        except Exception as e:
            print(f"[ymove] Log store error: {e}")

    if not db_pool or category != "subscription":
        return {"status": "ok", "received": event_type}

    async with db_pool.acquire() as conn:
        if event_type == "subscriptionCreated":
            await _ymove_handle_created(conn, event_data, email, ymove_user_id, provider, first_name, last_name)
        elif event_type == "subscriptionCancelled":
            await _ymove_handle_cancelled(conn, event_data, email, ymove_user_id)
        else:
            print(f"[ymove] Unhandled event type: {event_type}")

    return {"status": "ok", "processed": event_type, "email": email}


async def _ymove_lookup_utm(email: str, ymove_user_id: str = "") -> dict:
    """S21: Query ymove Member Lookup API for a user's meta parameters (UTM attribution).
    Returns dict with extracted UTM fields + raw meta response.
    Tosh confirmed: new checkout (ymove.app/join/movementandmiles) stores UTM params
    in the user's meta field. Only applies to Stripe signups (Apple/Google go through
    app stores which strip UTMs)."""
    result = {"found": False, "utm": {}, "raw_meta": None}
    if not YMOVE_API_KEY or not email:
        return result

    UTM_KEYS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'ym_source',
                'utm_id', 'gclid', 'fbclid', 'ttclid', 'msclkid', 'twclid', 'ref', 'referrer']
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                headers={"X-Authorization": YMOVE_API_KEY},
                params={"email": email.strip().lower()}
            )
            if resp.status_code != 200:
                print(f"[ymove-utm] Lookup failed for {email}: HTTP {resp.status_code}")
                return result

            data = resp.json()
            if not data.get("found"):
                print(f"[ymove-utm] User not found: {email}")
                return result

            user = data.get("user", {})
            result["found"] = True

            # Extract meta parameters -- try common field names
            meta = user.get("meta") or user.get("metadata") or user.get("metaParameters") or user.get("params") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}

            result["raw_meta"] = meta
            print(f"[ymove-utm] Raw meta for {email}: {json.dumps(meta, default=str)[:500]}")

            # Extract known UTM fields
            if isinstance(meta, dict):
                for key in UTM_KEYS:
                    val = meta.get(key, "")
                    if val:
                        result["utm"][key] = str(val)

            # Also check top-level user fields in case meta is nested differently
            for key in UTM_KEYS:
                if key not in result["utm"]:
                    val = user.get(key, "")
                    if val:
                        result["utm"][key] = str(val)

            if result["utm"]:
                print(f"[ymove-utm] Found UTMs for {email}: {result['utm']}")
            else:
                print(f"[ymove-utm] No UTM data found for {email} (meta keys: {list(meta.keys()) if isinstance(meta, dict) else 'not a dict'})")

    except Exception as e:
        print(f"[ymove-utm] Lookup error for {email}: {e}")

    return result


async def _store_utm_on_subscription(conn, stripe_sub_id: str, utm_result: dict):
    """S21: Store UTM attribution data on a subscription record."""
    utm = utm_result.get("utm", {})
    raw_meta = utm_result.get("raw_meta")

    if not utm and not raw_meta:
        return

    try:
        await conn.execute("""
            UPDATE subscriptions SET
                utm_source = COALESCE(NULLIF($1, ''), utm_source),
                utm_medium = COALESCE(NULLIF($2, ''), utm_medium),
                utm_campaign = COALESCE(NULLIF($3, ''), utm_campaign),
                utm_term = COALESCE(NULLIF($4, ''), utm_term),
                utm_content = COALESCE(NULLIF($5, ''), utm_content),
                ym_source = COALESCE(NULLIF($6, ''), ym_source),
                utm_meta_raw = $7,
                updated_at = NOW()
            WHERE stripe_subscription_id = $8
        """,
            utm.get("utm_source", ""),
            utm.get("utm_medium", ""),
            utm.get("utm_campaign", ""),
            utm.get("utm_term", ""),
            utm.get("utm_content", ""),
            utm.get("ym_source", ""),
            json.dumps(raw_meta, default=str) if raw_meta else None,
            stripe_sub_id
        )
        print(f"[ymove-utm] Stored UTM data on sub {stripe_sub_id}")
    except Exception as e:
        print(f"[ymove-utm] Store error for {stripe_sub_id}: {e}")


async def _ymove_handle_created(conn, event_data: dict, email: str, ymove_user_id: str, provider: str, first_name: str = "", last_name: str = ""):
    """Process subscriptionCreated from ymove."""
    now_ts = int(datetime.now(timezone.utc).timestamp())

    if provider == "stripe":
        # Stripe subs already arrive via direct Stripe webhook.
        # ymove adds email + name â attach email if missing.
        stripe_sub_id = event_data.get("stripeSubscriptionId", "")
        if not stripe_sub_id:
            return

        # Store event for audit
        eid = f"ymove_created_{stripe_sub_id}_{now_ts}"
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_subscription_id, source, data)
                   VALUES ($1, $2, $3, 'stripe', $4)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                eid, "ymove.subscriptionCreated", stripe_sub_id,
                json.dumps({"ymove_user_id": ymove_user_id, "email": email, "provider": provider}, default=str)
            )
        except Exception as e:
            print(f"[ymove] Stripe event store error: {e}")

        # Attach email if subscription exists but has no email
        if email:
            try:
                existing = await conn.fetchrow(
                    "SELECT id, email FROM subscriptions WHERE stripe_subscription_id = $1",
                    stripe_sub_id
                )
                if existing and not existing["email"]:
                    await conn.execute(
                        "UPDATE subscriptions SET email = $1, updated_at = NOW() WHERE stripe_subscription_id = $2",
                        email, stripe_sub_id
                    )
                    print(f"[ymove] Attached email {email} to Stripe sub {stripe_sub_id}")
                # S17: Store names
                if existing and (first_name or last_name):
                    try:
                        await conn.execute(
                            "UPDATE subscriptions SET first_name = COALESCE(NULLIF($1, ''), first_name), last_name = COALESCE(NULLIF($2, ''), last_name) WHERE stripe_subscription_id = $3",
                            first_name, last_name, stripe_sub_id
                        )
                    except Exception:
                        pass
            except Exception as e:
                print(f"[ymove] Stripe email attach error: {e}")

        # S21: Look up UTM attribution from ymove meta parameters
        if email:
            try:
                utm_result = await _ymove_lookup_utm(email, ymove_user_id)
                if utm_result["found"]:
                    await _store_utm_on_subscription(conn, stripe_sub_id, utm_result)
            except Exception as e:
                print(f"[ymove-utm] Attribution lookup error: {e}")

    elif provider == "apple":
        # Apple subs: ymove is our PRIMARY source (no direct Apple webhook connected)
        transaction_id = event_data.get("transactionId", "")
        if not transaction_id:
            return

        product_id = event_data.get("productId", "")
        start_str = event_data.get("startDate", "")
        end_str = event_data.get("endDate", "")

        # Determine plan from productId
        plan_interval = "month"
        plan_amount = 1999
        if product_id and ("annual" in product_id.lower() or "year" in product_id.lower()):
            plan_interval = "year"
            plan_amount = 17999

        # Parse ISO dates
        def _parse_iso(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        period_start = _parse_iso(start_str)
        period_end = _parse_iso(end_str)

        # Store event
        eid = f"ymove_created_{transaction_id}_{now_ts}"
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_subscription_id, source, data)
                   VALUES ($1, $2, $3, 'apple', $4)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                eid, "ymove.subscriptionCreated", transaction_id,
                json.dumps({"ymove_user_id": ymove_user_id, "email": email, "product_id": product_id}, default=str)
            )
        except Exception as e:
            print(f"[ymove] Apple event store error: {e}")

        # Upsert subscription â email + period dates are the key value from ymove
        try:
            await conn.execute("""
                INSERT INTO subscriptions (
                    stripe_customer_id, stripe_subscription_id, email, status,
                    plan_interval, plan_amount, currency, source,
                    current_period_start, current_period_end,
                    trial_start, trial_end,
                    created_at, updated_at
                ) VALUES ('', $1, $2, 'active', $3, $4, 'usd', 'apple', $5, $6, $5, $6, COALESCE($5, NOW()), NOW())
                ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                    email = COALESCE(NULLIF(EXCLUDED.email, ''), subscriptions.email),
                    status = 'active',
                    plan_interval = EXCLUDED.plan_interval,
                    plan_amount = EXCLUDED.plan_amount,
                    current_period_start = COALESCE(EXCLUDED.current_period_start, subscriptions.current_period_start),
                    current_period_end = COALESCE(EXCLUDED.current_period_end, subscriptions.current_period_end),
                    updated_at = NOW()
            """,
                transaction_id, email, plan_interval, plan_amount,
                period_start, period_end
            )
            print(f"[ymove] Apple sub upserted: {transaction_id} | {email}")
        except Exception as e:
            print(f"[ymove] Apple sub upsert error: {e}")

        # S17: Store names from ymove
        if first_name or last_name:
            try:
                await conn.execute(
                    "UPDATE subscriptions SET first_name = COALESCE(NULLIF($1, ''), first_name), last_name = COALESCE(NULLIF($2, ''), last_name) WHERE stripe_subscription_id = $3",
                    first_name, last_name, transaction_id
                )
            except Exception as e:
                print(f"[ymove] Apple name store error: {e}")

        # S18: Dedup - deactivate synthetic Meg-import record if real transaction arrived
        if email:
            try:
                synthetic = await conn.fetch(
                    """SELECT id, stripe_subscription_id FROM subscriptions
                       WHERE lower(email) = lower($1)
                       AND stripe_subscription_id LIKE 'meg_apple_%'
                       AND stripe_subscription_id != $2
                       AND status IN ('active', 'trialing')""",
                    email, transaction_id
                )
                for syn in synthetic:
                    await conn.execute(
                        "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                        syn["id"]
                    )
                    print(f"[ymove] Dedup: deactivated synthetic {syn['stripe_subscription_id']} for {email} (real: {transaction_id})")
            except Exception as e:
                print(f"[ymove] Apple dedup error: {e}")

        # S18: Detect trial-to-paid conversion (trial expired + still active + no cancel)
        try:
            conv_check = await conn.fetchrow(
                """SELECT id, trial_end FROM subscriptions
                   WHERE stripe_subscription_id = $1
                   AND converted_at IS NULL
                   AND status = 'active'
                   AND trial_end IS NOT NULL
                   AND trial_end < NOW()
                   AND canceled_at IS NULL""",
                transaction_id
            )
            if conv_check:
                await conn.execute(
                    "UPDATE subscriptions SET converted_at = $1 WHERE id = $2",
                    conv_check["trial_end"], conv_check["id"]
                )
                print(f"[ymove] Apple conversion detected for {transaction_id} (trial_end: {conv_check['trial_end']})")
        except Exception as e:
            print(f"[ymove] Apple conversion check error: {e}")

        # Assign readable_id if not yet set
        try:
            existing = await conn.fetchrow(
                "SELECT readable_id FROM subscriptions WHERE stripe_subscription_id = $1",
                transaction_id
            )
            if existing and not existing["readable_id"]:
                rid = await assign_readable_id(conn, "apple")
                await conn.execute(
                    "UPDATE subscriptions SET readable_id = $1 WHERE stripe_subscription_id = $2",
                    rid, transaction_id
                )
        except Exception as e:
            print(f"[ymove] Apple readable ID error: {e}")

    elif provider == "google":
        # Google subs â same pattern as Apple, using ymove uuid as ID
        ym_uuid = event_data.get("uuid", "")
        if not ym_uuid:
            return
        external_id = f"ym_google_{ym_uuid}"

        product_id = event_data.get("productId", "")
        start_str = event_data.get("startDate", "")
        end_str = event_data.get("endDate", "")

        plan_interval = "month"
        plan_amount = 1999
        if product_id and ("annual" in product_id.lower() or "year" in product_id.lower()):
            plan_interval = "year"
            plan_amount = 17999

        def _parse_iso_g(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        period_start = _parse_iso_g(start_str)
        period_end = _parse_iso_g(end_str)

        eid = f"ymove_created_{ym_uuid}_{now_ts}"
        try:
            await conn.execute(
                """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_subscription_id, source, data)
                   VALUES ($1, $2, $3, 'google', $4)
                   ON CONFLICT (stripe_event_id) DO NOTHING""",
                eid, "ymove.subscriptionCreated", external_id,
                json.dumps({"ymove_user_id": ymove_user_id, "email": email, "product_id": product_id}, default=str)
            )
        except Exception as e:
            print(f"[ymove] Google event store error: {e}")

        try:
            await conn.execute("""
                INSERT INTO subscriptions (
                    stripe_customer_id, stripe_subscription_id, email, status,
                    plan_interval, plan_amount, currency, source,
                    current_period_start, current_period_end,
                    created_at, updated_at
                ) VALUES ('', $1, $2, 'active', $3, $4, 'usd', 'google', $5, $6, COALESCE($5, NOW()), NOW())
                ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                    email = COALESCE(NULLIF(EXCLUDED.email, ''), subscriptions.email),
                    status = 'active',
                    updated_at = NOW()
            """,
                external_id, email, plan_interval, plan_amount,
                period_start, period_end
            )
            print(f"[ymove] Google sub upserted: {external_id} | {email}")
        except Exception as e:
            print(f"[ymove] Google sub upsert error: {e}")

        # S17: Store names from ymove
        if first_name or last_name:
            try:
                await conn.execute(
                    "UPDATE subscriptions SET first_name = COALESCE(NULLIF($1, ''), first_name), last_name = COALESCE(NULLIF($2, ''), last_name) WHERE stripe_subscription_id = $3",
                    first_name, last_name, external_id
                )
            except Exception as e:
                print(f"[ymove] Google name store error: {e}")

        # S18: Dedup - deactivate synthetic Meg-import record if real transaction arrived
        if email:
            try:
                synthetic = await conn.fetch(
                    """SELECT id, stripe_subscription_id FROM subscriptions
                       WHERE lower(email) = lower($1)
                       AND stripe_subscription_id LIKE 'meg_google_%'
                       AND stripe_subscription_id != $2
                       AND status IN ('active', 'trialing')""",
                    email, external_id
                )
                for syn in synthetic:
                    await conn.execute(
                        "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                        syn["id"]
                    )
                    print(f"[ymove] Dedup: deactivated synthetic {syn['stripe_subscription_id']} for {email} (real: {external_id})")
            except Exception as e:
                print(f"[ymove] Google dedup error: {e}")

        # S18: Detect trial-to-paid conversion (trial expired + still active + no cancel)
        try:
            conv_check = await conn.fetchrow(
                """SELECT id, trial_end FROM subscriptions
                   WHERE stripe_subscription_id = $1
                   AND converted_at IS NULL
                   AND status = 'active'
                   AND trial_end IS NOT NULL
                   AND trial_end < NOW()
                   AND canceled_at IS NULL""",
                external_id
            )
            if conv_check:
                await conn.execute(
                    "UPDATE subscriptions SET converted_at = $1 WHERE id = $2",
                    conv_check["trial_end"], conv_check["id"]
                )
                print(f"[ymove] Google conversion detected for {external_id} (trial_end: {conv_check['trial_end']})")
        except Exception as e:
            print(f"[ymove] Google conversion check error: {e}")

        # Assign readable_id
        try:
            existing = await conn.fetchrow(
                "SELECT readable_id FROM subscriptions WHERE stripe_subscription_id = $1",
                external_id
            )
            if existing and not existing["readable_id"]:
                rid = await assign_readable_id(conn, "google")
                await conn.execute(
                    "UPDATE subscriptions SET readable_id = $1 WHERE stripe_subscription_id = $2",
                    rid, external_id
                )
        except Exception as e:
            print(f"[ymove] Google readable ID error: {e}")

    else:
        print(f"[ymove] Unknown provider: {provider}")


async def _ymove_handle_cancelled(conn, event_data: dict, email: str, ymove_user_id: str):
    """Process subscriptionCancelled from ymove.
    Cancel events have no provider field â we find the sub by email."""
    now_ts = int(datetime.now(timezone.utc).timestamp())

    # Store event for audit
    eid = f"ymove_cancelled_{ymove_user_id}_{now_ts}"
    try:
        await conn.execute(
            """INSERT INTO subscription_events (stripe_event_id, event_type, stripe_customer_id, stripe_subscription_id, source, data)
               VALUES ($1, $2, '', '', 'ymove', $3)
               ON CONFLICT (stripe_event_id) DO NOTHING""",
            eid, "ymove.subscriptionCancelled",
            json.dumps({"ymove_user_id": ymove_user_id, "email": email, "event": event_data}, default=str)
        )
    except Exception as e:
        print(f"[ymove] Cancel event store error: {e}")

    if not email:
        print("[ymove] Cancel event has no email â cannot match subscription")
        return

    # Find most recent active sub for this email and cancel it
    # For Stripe subs, the direct Stripe webhook will also fire â double-cancel is safe (no-op)
    # For Apple subs, this is the ONLY cancel signal we get
    try:
        active_sub = await conn.fetchrow(
            """SELECT id, stripe_subscription_id, source FROM subscriptions
               WHERE lower(email) = lower($1) AND status IN ('active', 'trialing')
               ORDER BY created_at DESC LIMIT 1""",
            email
        )
        if active_sub:
            await conn.execute(
                "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                active_sub["id"]
            )
            print(f"[ymove] Cancelled {active_sub['source']} sub {active_sub['stripe_subscription_id']} for {email}")
        else:
            print(f"[ymove] No active sub found for {email} to cancel")
    except Exception as e:
        print(f"[ymove] Cancel processing error: {e}")


# --- Daily Digest System ---

async def gather_daily_stats() -> dict:
    """Query database for last 24 hours of activity."""
    if not db_pool:
        return {"error": "No database"}

    async with db_pool.acquire() as conn:
        # New subscriptions in last 24h
        new_subs = await conn.fetch(
            """SELECT source, status, plan_interval, plan_amount, email, created_at
               FROM subscriptions WHERE created_at > NOW() - INTERVAL '24 hours'
               AND created_at <= NOW()
               ORDER BY created_at DESC"""
        )
        new_subs_by_source = await conn.fetch(
            """SELECT source, COUNT(*) as count FROM subscriptions
               WHERE created_at > NOW() - INTERVAL '24 hours'
               AND created_at <= NOW()
               GROUP BY source ORDER BY count DESC"""
        )

        # Conversions (trial â paid) in last 24h
        conversions_today = await conn.fetch(
            """SELECT source, email, plan_interval, plan_amount, converted_at
               FROM subscriptions WHERE converted_at > NOW() - INTERVAL '24 hours'
               ORDER BY converted_at DESC"""
        )

        # Cancellations in last 24h (S22: include converted_at to distinguish paid vs trial)
        cancellations = await conn.fetch(
            """SELECT source, email, canceled_at, converted_at, trial_end FROM subscriptions
               WHERE canceled_at > NOW() - INTERVAL '24 hours'
               ORDER BY canceled_at DESC"""
        )

        # Current MRR
        mrr_monthly = await conn.fetchval(
            "SELECT COALESCE(SUM(plan_amount), 0) FROM subscriptions WHERE status = 'active' AND plan_interval = 'month'"
        )
        mrr_annual = await conn.fetchval(
            "SELECT COALESCE(SUM(plan_amount / 12), 0) FROM subscriptions WHERE status = 'active' AND plan_interval = 'year'"
        )
        current_mrr_cents = (mrr_monthly or 0) + (mrr_annual or 0)

        # MRR by source (for fee calc)
        mrr_by_source = await conn.fetch(
            """SELECT source,
                COALESCE(SUM(CASE WHEN plan_interval='month' THEN plan_amount ELSE 0 END), 0) +
                COALESCE(SUM(CASE WHEN plan_interval='year' THEN plan_amount/12 ELSE 0 END), 0) as mrr_cents
               FROM subscriptions WHERE status = 'active' GROUP BY source"""
        )

        # Active subs count
        active_count = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE status IN ('active', 'trialing')"
        )
        trialing_count = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE status = 'trialing'"
        )
        total_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

        # New leads in last 24h
        new_leads = await conn.fetch(
            """SELECT first_name, email, utm_source, utm_medium, utm_campaign, created_at
               FROM leads WHERE created_at > NOW() - INTERVAL '24 hours'
               ORDER BY created_at DESC"""
        )
        leads_by_source_24h = await conn.fetch(
            """SELECT COALESCE(NULLIF(utm_source,''), 'direct') as source, COUNT(*) as count
               FROM leads WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY source ORDER BY count DESC"""
        )

        # Page views last 24h
        pv_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM page_views WHERE created_at > NOW() - INTERVAL '24 hours'"
        )
        pv_by_page = await conn.fetch(
            """SELECT page, COUNT(*) as count FROM page_views
               WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY page ORDER BY count DESC LIMIT 5"""
        )

        # Chat sessions last 24h
        chats_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM chat_sessions WHERE created_at > NOW() - INTERVAL '24 hours'"
        )

        # Subscription events in last 24h
        recent_events = await conn.fetch(
            """SELECT event_type, source, created_at FROM subscription_events
               WHERE created_at > NOW() - INTERVAL '24 hours'
               ORDER BY created_at DESC LIMIT 30"""
        )

    # Calculate fees
    fee_breakdown = {}
    total_fees_cents = 0
    for r in mrr_by_source:
        src = r["source"]
        mrr = r["mrr_cents"] or 0
        if src == "apple":
            fee = round(mrr * 0.15)
        elif src == "google":
            fee = round(mrr * 0.15)
        else:
            fee = round(mrr * 0.029)
        fee_breakdown[src] = fee
        total_fees_cents += fee
    net_mrr_cents = current_mrr_cents - total_fees_cents

    return {
        "conversions_today": len(conversions_today),
        "conversion_details": [{"email": r["email"] or "n/a", "source": r["source"], "plan": f"${(r['plan_amount'] or 0)/100:.2f}/{r['plan_interval'] or '?'}"} for r in conversions_today],
        "new_subscriptions": len(new_subs),
        "new_subs_by_source": [{"source": r["source"], "count": r["count"]} for r in new_subs_by_source],
        "new_sub_details": [{"email": r["email"] or "n/a", "source": r["source"], "plan": f"${(r['plan_amount'] or 0)/100:.2f}/{r['plan_interval'] or '?'}"} for r in new_subs],
        "cancellations": len(cancellations),
        "cancellations_paid": len([c for c in cancellations if c["converted_at"] is not None]),
        "cancellations_trial": len([c for c in cancellations if c["converted_at"] is None]),
        "cancel_details": [{"email": r["email"] or "n/a", "source": r["source"], "type": "paid" if r["converted_at"] is not None else "trial"} for r in cancellations],
        "gross_mrr": f"${current_mrr_cents/100:,.2f}",
        "gross_mrr_cents": current_mrr_cents,
        "net_mrr": f"${net_mrr_cents/100:,.2f}",
        "net_mrr_cents": net_mrr_cents,
        "total_fees": f"${total_fees_cents/100:,.2f}",
        "fee_breakdown": fee_breakdown,
        "active_subscribers": active_count or 0,
        "trialing": trialing_count or 0,
        "total_subscribers": total_count or 0,
        "new_leads": len(new_leads),
        "leads_by_source": [{"source": r["source"], "count": r["count"]} for r in leads_by_source_24h],
        "page_views_24h": pv_24h or 0,
        "top_pages": [{"page": r["page"], "count": r["count"]} for r in pv_by_page],
        "chat_sessions_24h": chats_24h or 0,
        "events_24h": len(recent_events),
        "mrr_by_source": [{"source": r["source"], "mrr_cents": r["mrr_cents"] or 0} for r in mrr_by_source],
    }


async def generate_digest_insights(stats: dict) -> str:
    """Send daily stats to Claude for analysis and insights."""
    # Trim to key metrics only (avoid oversized prompt)
    slim = {
        "conversions_today": stats.get("conversions_today", 0),
        "new_subscriptions_trial_starts": stats.get("new_subscriptions", 0),
        "new_subs_by_source": stats.get("new_subs_by_source", []),
        "cancellations": stats.get("cancellations", 0),
        "cancellations_paid_subscribers": stats.get("cancellations_paid", 0),
        "cancellations_trial_users": stats.get("cancellations_trial", 0),
        "gross_mrr": stats.get("gross_mrr", "$0"),
        "net_mrr": stats.get("net_mrr", "$0"),
        "total_fees": stats.get("total_fees", "$0"),
        "active_subscribers": stats.get("active_subscribers", 0),
        "trialing": stats.get("trialing", 0),
        "new_leads": stats.get("new_leads", 0),
        "leads_by_source": stats.get("leads_by_source", []),
        "page_views_24h": stats.get("page_views_24h", 0),
        "top_pages": stats.get("top_pages", []),
        "chat_sessions_24h": stats.get("chat_sessions_24h", 0),
        "mrr_by_source": stats.get("mrr_by_source", []),
        "fee_breakdown": stats.get("fee_breakdown", {}),
    }
    stats_text = json.dumps(slim, indent=2, default=str)
    prompt = f"Here are today's metrics for Movement & Miles (fitness subscription app). Generate a brief morning digest with key insights and any recommended actions.\n\n{stats_text}"
    print(f"[Digest] Prompt length: {len(prompt)} chars")
    return await call_anthropic_raw(DIGEST_SYSTEM_PROMPT, [{"role": "user", "content": prompt}], max_tokens=500)


def build_digest_html(stats: dict, insights: str) -> str:
    """Build branded HTML email for the daily digest."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    date_str = now_et.strftime("%A, %B %d, %Y")

    # Build new subs detail rows
    sub_rows = ""
    for s in stats.get("new_sub_details", [])[:10]:
        sub_rows += f'<tr><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{s["email"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{s["source"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{s["plan"]}</td></tr>'

    cancel_rows = ""
    for c in stats.get("cancel_details", [])[:10]:
        ctype = c.get("type", "unknown")
        type_color = "#c0392b" if ctype == "paid" else "#b35a00"
        type_label = "PAID" if ctype == "paid" else "TRIAL"
        cancel_rows += f'<tr><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{c["email"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{c["source"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0;color:{type_color};font-weight:600">{type_label}</td></tr>'

    lead_source_rows = ""
    for ls in stats.get("leads_by_source", []):
        lead_source_rows += f'<tr><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{ls["source"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{ls["count"]}</td></tr>'

    # Session 11: Convert markdown bold **text** to <strong>text</strong>
    insights_clean = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', insights)
    # Format insights â convert newlines and bullet points to HTML
    insights_html = insights_clean.replace("\n\n", "</p><p style='margin:0 0 12px'>").replace("\n- ", "<br>&#8226; ").replace("\n", "<br>")

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f7f7f7;font-family:Arial,Helvetica,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f7f7;padding:24px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">

<!-- Header -->
<tr><td style="background:#182241;padding:28px 32px">
  <h1 style="margin:0;color:#ffffff;font-size:22px;font-weight:600">Movement &amp; Miles</h1>
  <p style="margin:6px 0 0;color:rgba(255,255,255,0.7);font-size:13px">Daily Digest &mdash; {date_str}</p>
</td></tr>

<!-- AI Insights -->
<tr><td style="padding:28px 32px 20px">
  <h2 style="margin:0 0 12px;color:#182241;font-size:16px;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Today&rsquo;s Insights</h2>
  <div style="background:#f0f4f8;border-left:4px solid #182241;border-radius:0 8px 8px 0;padding:16px 20px;font-size:14px;line-height:1.6;color:#333">
    <p style="margin:0 0 12px">{insights_html}</p>
  </div>
</td></tr>

<!-- Key Metrics -->
<tr><td style="padding:0 32px 24px">
  <h2 style="margin:0 0 16px;color:#182241;font-size:16px;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Revenue Snapshot</h2>
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td style="padding:16px;background:#f7f7f7;border-radius:8px;text-align:center;width:25%">
        <div style="font-size:11px;color:#536c7c;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Gross MRR</div>
        <div style="font-size:24px;font-weight:700;color:#2d6a2d;margin-top:4px">{stats.get('gross_mrr','$0')}</div>
      </td>
      <td width="12"></td>
      <td style="padding:16px;background:#f7f7f7;border-radius:8px;text-align:center;width:25%">
        <div style="font-size:11px;color:#536c7c;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Net MRR</div>
        <div style="font-size:24px;font-weight:700;color:#2d6a2d;margin-top:4px">{stats.get('net_mrr','$0')}</div>
      </td>
      <td width="12"></td>
      <td style="padding:16px;background:#f7f7f7;border-radius:8px;text-align:center;width:25%">
        <div style="font-size:11px;color:#536c7c;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Active Subs</div>
        <div style="font-size:24px;font-weight:700;color:#182241;margin-top:4px">{stats.get('active_subscribers',0)}</div>
      </td>
      <td width="12"></td>
      <td style="padding:16px;background:#f7f7f7;border-radius:8px;text-align:center;width:25%">
        <div style="font-size:11px;color:#536c7c;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Fees / Mo</div>
        <div style="font-size:24px;font-weight:700;color:#b35a00;margin-top:4px">{stats.get('total_fees','$0')}</div>
      </td>
    </tr>
  </table>
</td></tr>

<!-- 24h Activity -->
<tr><td style="padding:0 32px 24px">
  <h2 style="margin:0 0 16px;color:#182241;font-size:16px;font-weight:600;text-transform:uppercase;letter-spacing:0.05em">Last 24 Hours</h2>
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td style="padding:12px 16px;background:#e8f4e8;border-radius:8px;text-align:center;width:20%">
        <div style="font-size:28px;font-weight:700;color:#2d6a2d">{stats.get('new_subscriptions',0)}</div>
        <div style="font-size:11px;color:#2d6a2d;font-weight:600;margin-top:2px">Trial Starts</div>
      </td>
      <td width="12"></td>
      <td style="padding:12px 16px;background:#e8f7e8;border-radius:8px;text-align:center;width:20%">
        <div style="font-size:28px;font-weight:700;color:#1b7a1b">{stats.get('conversions_today',0)}</div>
        <div style="font-size:11px;color:#1b7a1b;font-weight:600;margin-top:2px">Paid Conversions</div>
      </td>
      <td width="12"></td>
      <td style="padding:12px 16px;background:#fde8e8;border-radius:8px;text-align:center;width:20%">
        <div style="font-size:28px;font-weight:700;color:#c0392b">{stats.get('cancellations',0)}</div>
        <div style="font-size:11px;color:#c0392b;font-weight:600;margin-top:2px">Cancellations</div>
        <div style="font-size:10px;color:#c0392b;margin-top:2px">{stats.get('cancellations_paid',0)} paid &bull; {stats.get('cancellations_trial',0)} trial</div>
      </td>
      <td width="12"></td>
      <td style="padding:12px 16px;background:#e8eaf6;border-radius:8px;text-align:center;width:20%">
        <div style="font-size:28px;font-weight:700;color:#3949ab">{stats.get('new_leads',0)}</div>
        <div style="font-size:11px;color:#3949ab;font-weight:600;margin-top:2px">New Leads</div>
      </td>
      <td width="12"></td>
      <td style="padding:12px 16px;background:#f7f7f7;border-radius:8px;text-align:center;width:25%">
        <div style="font-size:28px;font-weight:700;color:#182241">{stats.get('page_views_24h',0)}</div>
        <div style="font-size:11px;color:#536c7c;font-weight:600;margin-top:2px">Page Views</div>
      </td>
    </tr>
  </table>
</td></tr>"""

    # New subscriptions detail table (only if there are any)
    if stats.get("new_subscriptions", 0) > 0:
        html += f"""
<!-- New Subscriptions Detail -->
<tr><td style="padding:0 32px 24px">
  <h3 style="margin:0 0 8px;color:#2d6a2d;font-size:14px;font-weight:600">New Subscriptions</h3>
  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
    <tr style="background:#f7f7f7"><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Email</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Source</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Plan</th></tr>
    {sub_rows}
  </table>
</td></tr>"""

    # Cancellations detail (only if there are any)
    if stats.get("cancellations", 0) > 0:
        html += f"""
<!-- Cancellations Detail -->
<tr><td style="padding:0 32px 24px">
  <h3 style="margin:0 0 8px;color:#c0392b;font-size:14px;font-weight:600">Cancellations ({stats.get('cancellations_paid',0)} paid, {stats.get('cancellations_trial',0)} trial)</h3>
  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
    <tr style="background:#f7f7f7"><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Email</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Source</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Type</th></tr>
    {cancel_rows}
  </table>
</td></tr>"""

    # Lead sources (only if there are leads)
    if stats.get("new_leads", 0) > 0 and lead_source_rows:
        html += f"""
<!-- Lead Sources -->
<tr><td style="padding:0 32px 24px">
  <h3 style="margin:0 0 8px;color:#3949ab;font-size:14px;font-weight:600">Lead Sources (24h)</h3>
  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
    <tr style="background:#f7f7f7"><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Source</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Count</th></tr>
    {lead_source_rows}
  </table>
</td></tr>"""

    html += f"""
<!-- Footer -->
<tr><td style="padding:20px 32px 28px;border-top:1px solid #e0e0e0">
  <p style="margin:0;font-size:12px;color:#536c7c;text-align:center">
    Movement &amp; Miles Admin Dashboard &mdash;
    <a href="https://movementmiles2-production.up.railway.app/mm-admin" style="color:#182241;text-decoration:underline">Open Dashboard</a>
  </p>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    return html


async def send_digest_email(html: str, subject: str) -> dict:
    """Send email via Resend API."""
    if not RESEND_API_KEY:
        return {"error": "RESEND_API_KEY not configured"}
    if not DIGEST_RECIPIENTS:
        return {"error": "DIGEST_RECIPIENTS not configured"}

    recipients = [e.strip() for e in DIGEST_RECIPIENTS.split(",") if e.strip()]
    if not recipients:
        return {"error": "No valid recipients"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": DIGEST_FROM_EMAIL,
                    "to": recipients,
                    "subject": subject,
                    "html": html,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return {"status": "sent", "id": data.get("id", ""), "recipients": recipients}
        except Exception as e:
            return {"error": f"Resend API error: {str(e)}"}


async def run_daily_digest():
    """Orchestrator: auto-stamp conversions -> gather stats -> AI insights -> build email -> send."""
    print(f"[Digest] Starting daily digest at {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M ET')}")
    try:
        # S18: Auto-stamp Apple/Google conversions (trial expired + active + no cancel)
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    result = await conn.execute(
                        """UPDATE subscriptions SET converted_at = trial_end
                           WHERE converted_at IS NULL
                           AND source IN ('apple', 'google')
                           AND status = 'active'
                           AND trial_end IS NOT NULL
                           AND trial_end < NOW()
                           AND canceled_at IS NULL"""
                    )
                    stamped = int(result.split(" ")[-1]) if result else 0
                    if stamped > 0:
                        print(f"[Digest] Auto-stamped {stamped} Apple/Google conversions")
            except Exception as e:
                print(f"[Digest] Auto-stamp error: {e}")

        stats = await gather_daily_stats()
        if "error" in stats:
            print(f"[Digest] Error gathering stats: {stats['error']}")
            return

        insights = await generate_digest_insights(stats)

        now_et = datetime.now(ZoneInfo("America/New_York"))
        subject = f"M&M Daily Digest - {now_et.strftime('%b %d')} | {stats.get('conversions_today',0)} conversions, {stats.get('new_subscriptions',0)} trials, {stats.get('cancellations_paid',0)} paid cancels, {stats.get('cancellations_trial',0)} trial cancels"

        html = build_digest_html(stats, insights)
        result = await send_digest_email(html, subject)

        if "error" in result:
            print(f"[Digest] Send error: {result['error']}")
        else:
            print(f"[Digest] Sent successfully to {result['recipients']} (id: {result.get('id','')})")
    except Exception as e:
        print(f"[Digest] Unexpected error: {e}")


# --- Admin Endpoints ---

@app.post("/api/admin/login")
async def admin_login(req: LoginRequest):
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"status": "ok"}


@app.post("/api/admin/fix-future-dates")
async def fix_future_dates(request: Request):
    """Fix subscriptions with created_at in the future (e.g. 2026-12-31 from bad import)."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        # Find future-dated subs
        future_subs = await conn.fetch(
            "SELECT id, stripe_subscription_id, email, source, created_at FROM subscriptions WHERE created_at > NOW()"
        )
        if not future_subs:
            return {"status": "ok", "fixed": 0, "message": "No future-dated subscriptions found"}

        # Set created_at to their trial_start, or current_period_start, or updated_at as fallback
        fixed = 0
        details = []
        for s in future_subs:
            result = await conn.execute(
                """UPDATE subscriptions SET created_at = COALESCE(trial_start, current_period_start, updated_at, NOW())
                   WHERE id = $1 AND created_at > NOW()""",
                s["id"]
            )
            if result and result.endswith("1"):
                fixed += 1
                details.append({"email": s["email"] or "n/a", "source": s["source"], "old_date": str(s["created_at"])})

    return {"status": "ok", "fixed": fixed, "total_found": len(future_subs), "details": details}


@app.post("/api/admin/smart-backfill-conversions")
async def smart_backfill_conversions(request: Request):
    """S18: Stamp converted_at on Apple/Google subs that survived past their trial.
    Heuristic: trial_end has passed + still active + never cancelled = converted.
    Stamps converted_at = trial_end. Only where converted_at is currently NULL."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    preview = body.get("preview", True)

    async with db_pool.acquire() as conn:
        candidates = await conn.fetch(
            """SELECT id, stripe_subscription_id, email, source, trial_end, created_at
               FROM subscriptions
               WHERE converted_at IS NULL
               AND source IN ('apple', 'google')
               AND status = 'active'
               AND trial_end IS NOT NULL
               AND trial_end < NOW()
               AND canceled_at IS NULL
               ORDER BY source, trial_end"""
        )

        if preview:
            by_source = {}
            in_30d = 0
            for r in candidates:
                src = r["source"]
                by_source[src] = by_source.get(src, 0) + 1
                if r["trial_end"] and (datetime.now(timezone.utc) - r["trial_end"]).days <= 30:
                    in_30d += 1
            return {
                "status": "preview",
                "would_stamp": len(candidates),
                "by_source": by_source,
                "would_show_in_30d_count": in_30d,
                "sample": [
                    {"email": r["email"] or "n/a", "source": r["source"],
                     "trial_end": str(r["trial_end"]), "created_at": str(r["created_at"])}
                    for r in candidates[:20]
                ],
                "note": "These subs are active, past trial, never cancelled. converted_at will be set to trial_end. Check would_show_in_30d_count for dashboard impact."
            }

        stamped = 0
        for r in candidates:
            await conn.execute(
                "UPDATE subscriptions SET converted_at = $1 WHERE id = $2",
                r["trial_end"], r["id"]
            )
            stamped += 1

    return {"status": "ok", "stamped": stamped, "note": "converted_at set to trial_end for active subs that survived past trial."}


@app.post("/api/admin/cleanup-converted-at")
async def cleanup_converted_at(request: Request):
    """S18: NULL out fabricated converted_at on Apple/Google subs from S16 backfill.
    These dates were synthetic (trial_end = created_at + 30d) and pollute conversion metrics."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    preview = body.get("preview", True)

    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE source IN ('apple', 'google') AND converted_at IS NOT NULL"
        )
        if preview:
            by_source = await conn.fetch(
                """SELECT source, COUNT(*) as cnt FROM subscriptions
                   WHERE source IN ('apple', 'google') AND converted_at IS NOT NULL
                   GROUP BY source"""
            )
            return {
                "status": "preview",
                "would_null": count,
                "by_source": [{"source": r["source"], "count": r["cnt"]} for r in by_source],
                "note": "Send preview: false to execute. This NULLs converted_at on Apple/Google subs where dates were fabricated by S16 backfill."
            }
        result = await conn.execute(
            "UPDATE subscriptions SET converted_at = NULL WHERE source IN ('apple', 'google') AND converted_at IS NOT NULL"
        )
        cleaned = int(result.split(" ")[-1]) if result else 0

    return {"status": "ok", "cleaned": cleaned, "note": "converted_at NULLed on Apple/Google subs. Conversion metrics now reflect Stripe-only data."}


@app.post("/api/admin/backfill-names")
async def backfill_names(request: Request):
    """Backfill first_name/last_name on subscriptions from leads table."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE subscriptions s SET
                first_name = COALESCE(NULLIF(s.first_name, ''), l.first_name),
                last_name = COALESCE(NULLIF(s.last_name, ''), l.extra)
            FROM leads l
            WHERE lower(s.email) = lower(l.email)
            AND s.email != '' AND l.email != ''
            AND (s.first_name IS NULL OR s.first_name = '' OR s.last_name IS NULL OR s.last_name = '')
        """)
        count = int(result.split(" ")[-1]) if result else 0

    return {"status": "ok", "updated": count}


@app.get("/api/admin/ymove-log")
async def admin_ymove_log(request: Request):
    """View recent ymove webhook payloads for debugging."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM ymove_webhook_log ORDER BY created_at DESC LIMIT 50"
        )

    return {"events": [
        {"id": r["id"], "event_type": r["event_type"], "payload": r["payload"], "created_at": str(r["created_at"])}
        for r in rows
    ]}


# --- Session 17: ymove API Verification ---

@app.post("/api/admin/ymove-verify")
async def ymove_verify(request: Request):
    """Verify emails against ymove Member Lookup API (S19 rewrite).
    Modes:
      {"smoke_test": true} - test API connectivity with Meg's email
      {"email": "single@ex.com"} - look up one email
      {"emails": [...]} - batch lookup with rate limiting
      {"pull_all_subscribed": true} - pull ALL active members, cross-ref our cancelled subs
    """
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    ymove_key = YMOVE_API_KEY
    if not ymove_key:
        return JSONResponse(status_code=500, content={
            "error": "YMOVE_API_KEY not set in environment variables."
        })

    body = await request.json()

    # Smoke test mode
    if body.get("smoke_test"):
        test_email = body.get("email", "takacsmeghan@gmail.com")
        return await _ymove_smoke_test(ymove_key, test_email)

    # Pull all subscribed members and cross-reference
    if body.get("pull_all_subscribed"):
        return await _ymove_pull_all_subscribed(ymove_key)

    # Single or batch email lookup
    emails = body.get("emails", [])
    if not emails and body.get("email"):
        emails = [body["email"]]
    if not emails:
        return JSONResponse(status_code=400, content={
            "error": "Provide email, emails, smoke_test, or pull_all_subscribed"
        })

    batch_size = min(body.get("batch_size", 10), 50)
    delay_seconds = max(body.get("delay_seconds", 1.5), 0.5)

    results = []
    rate_limit_hits = 0
    errors = 0
    total_delay = 0.0

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i, email in enumerate(emails):
            if i > 0 and i % batch_size == 0:
                await asyncio.sleep(delay_seconds)
                total_delay += delay_seconds
            try:
                resp = await client.get(
                    f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                    headers={"X-Authorization": ymove_key},
                    params={"email": email.strip().lower()}
                )
                if resp.status_code == 429:
                    rate_limit_hits += 1
                    retry_after = float(resp.headers.get("retry-after", "5"))
                    print(f"[ymove] Rate limited on {email}, backing off {retry_after}s")
                    await asyncio.sleep(retry_after)
                    total_delay += retry_after
                    resp = await client.get(
                        f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                        headers={"X-Authorization": ymove_key},
                        params={"email": email.strip().lower()}
                    )
                if resp.status_code == 200:
                    data = resp.json()
                    status = _ymove_parse_status(data)
                    results.append({"email": email, "status": status, "raw": data})
                elif resp.status_code == 404:
                    results.append({"email": email, "status": "not_found", "raw": None})
                else:
                    results.append({
                        "email": email, "status": "error",
                        "http_status": resp.status_code, "raw": resp.text[:300]
                    })
                    errors += 1
            except Exception as e:
                results.append({"email": email, "status": "error", "error": str(e)})
                errors += 1

    active_count = sum(1 for r in results if r["status"] == "active")
    expired_count = sum(1 for r in results if r["status"] == "expired")
    not_found_count = sum(1 for r in results if r["status"] == "not_found")

    return {
        "total": len(emails), "verified": len(results),
        "active": active_count, "expired": expired_count,
        "not_found": not_found_count, "errors": errors,
        "rate_limit_hits": rate_limit_hits,
        "total_delay_seconds": round(total_delay, 1),
        "results": results
    }


async def _ymove_smoke_test(api_key: str, test_email: str) -> dict:
    """Quick connectivity test against ymove member-lookup with siteId 75."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            url = f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup"
            resp = await client.get(
                url,
                headers={"X-Authorization": api_key},
                params={"email": test_email}
            )
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            return {
                "status": "smoke_test_complete",
                "test_email": test_email,
                "http_status": resp.status_code,
                "api_url": url,
                "response": body,
                "success": resp.status_code == 200,
                "next_step": "If success, try {\"pull_all_subscribed\": true} to cross-ref candidates"
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


async def _ymove_pull_all_subscribed(api_key: str) -> dict:
    """Pull all subscribed members from ymove paginated API, cross-ref with our cancelled Apple/Google subs."""
    if not db_pool:
        return {"error": "No database connected"}

    # Step 1: Pull all subscribed members page by page
    all_members = []
    page = 1
    total_pages = None
    rate_limit_hits = 0
    last_data = {}

    async with httpx.AsyncClient(timeout=20.0) as client:
        while True:
            try:
                resp = await client.get(
                    f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup/all",
                    headers={"X-Authorization": api_key},
                    params={"status": "subscribed", "page": str(page)}
                )
                if resp.status_code == 429:
                    rate_limit_hits += 1
                    retry_after = float(resp.headers.get("retry-after", "5"))
                    print(f"[ymove] Rate limited on page {page}, backing off {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status_code != 200:
                    return {
                        "error": f"ymove API returned {resp.status_code} on page {page}",
                        "body": resp.text[:500],
                        "members_so_far": len(all_members)
                    }
                last_data = resp.json()
                users = last_data.get("users", [])
                all_members.extend(users)
                total_pages = last_data.get("totalPages", 1)
                print(f"[ymove] Page {page}/{total_pages}: {len(users)} members (total: {len(all_members)})")
                if page >= total_pages:
                    break
                page += 1
                await asyncio.sleep(1.0)
            except Exception as e:
                return {"error": f"Failed on page {page}: {str(e)}", "members_so_far": len(all_members)}

    # Step 2: Build lookup of ymove active emails
    ymove_active = {}
    for m in all_members:
        email = (m.get("email") or "").strip().lower()
        if email:
            ymove_active[email] = {
                "provider": m.get("subscriptionProvider"),
                "plan_id": m.get("activeSubscriptionPlanID"),
                "first_name": m.get("firstName"),
                "last_name": m.get("lastName"),
                "signup_date": m.get("signupDate"),
            }

    # Step 3: Cross-ref with our cancelled Apple/Google subs
    async with db_pool.acquire() as conn:
        cancelled_rows = await conn.fetch("""
            SELECT DISTINCT ON (lower(email))
                id, lower(email) as em, source, status, canceled_at, created_at,
                stripe_subscription_id, first_name, last_name
            FROM subscriptions
            WHERE status = 'canceled' AND source IN ('apple', 'google') AND email != ''
            ORDER BY lower(email), created_at DESC
        """)
        active_emails = set(r["em"] for r in await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status IN ('active', 'trialing')"
        ))

    # Step 4: Categorize
    reactivation_candidates = []
    already_active = 0
    confirmed_cancelled = 0

    for row in cancelled_rows:
        email = row["em"]
        if email in active_emails:
            already_active += 1
            continue
        if email in ymove_active:
            ym = ymove_active[email]
            reactivation_candidates.append({
                "email": email,
                "db_source": row["source"],
                "db_sub_id": row["stripe_subscription_id"],
                "db_canceled_at": str(row["canceled_at"] or ""),
                "ymove_provider": ym["provider"],
                "ymove_plan_id": ym["plan_id"],
                "ymove_first_name": ym["first_name"],
                "ymove_last_name": ym["last_name"],
            })
        else:
            confirmed_cancelled += 1

    reactivation_candidates.sort(key=lambda x: x["email"])
    react_emails = [c["email"] for c in reactivation_candidates]

    return {
        "ymove_total_subscribed": last_data.get("total", len(all_members)),
        "ymove_pages_fetched": page,
        "ymove_unique_emails": len(ymove_active),
        "our_cancelled_apple_google": len(cancelled_rows),
        "already_active_in_db": already_active,
        "reactivation_candidates": len(reactivation_candidates),
        "confirmed_cancelled_in_ymove_too": confirmed_cancelled,
        "rate_limit_hits": rate_limit_hits,
        "candidates": reactivation_candidates,
        "candidate_emails": react_emails,
        "next_step": f"Review {len(reactivation_candidates)} candidates, then POST /api/admin/ymove-reactivate with emails list (preview first)"
    }


def _ymove_parse_status(data: dict) -> str:
    """Parse ymove member-lookup response. Format: {found: bool, user: {activeSubscription: bool, ...}}"""
    if not isinstance(data, dict):
        return "unknown"
    if not data.get("found"):
        return "not_found"
    user = data.get("user", {})
    if user.get("activeSubscription") is True:
        return "active"
    if user.get("previouslySubscribed") is True:
        return "expired"
    return "not_found"


@app.post("/api/admin/ymove-reactivate")
async def ymove_reactivate(request: Request):
    """Reactivate confirmed-active subs from ymove verification.
    Body: {"emails": [...], "preview": true/false}
    Only reactivates Apple/Google source subs (Stripe webhook is authoritative)."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = await request.json()
    emails = body.get("emails", [])
    if not emails:
        return JSONResponse(status_code=400, content={"error": "Provide emails list"})

    batch_id = body.get("batch_id", f"ymove_react_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
    preview = body.get("preview", True)

    async with db_pool.acquire() as conn:
        reactivated = 0
        skipped = 0
        details = []
        for email in emails:
            email_lower = email.strip().lower()
            row = await conn.fetchrow("""
                SELECT id, stripe_subscription_id, source, status, canceled_at
                FROM subscriptions
                WHERE lower(email) = $1 AND status = 'canceled' AND source IN ('apple', 'google')
                ORDER BY created_at DESC LIMIT 1
            """, email_lower)
            if not row:
                skipped += 1
                details.append({"email": email_lower, "action": "skipped", "reason": "no cancelled apple/google sub"})
                continue
            if preview:
                details.append({"email": email_lower, "action": "would_reactivate", "sub_id": row["stripe_subscription_id"], "source": row["source"], "canceled_at": str(row["canceled_at"] or "")})
                reactivated += 1
            else:
                await conn.execute("UPDATE subscriptions SET status = 'active', canceled_at = NULL, updated_at = NOW(), import_batch = $1 WHERE id = $2", batch_id, row["id"])
                reactivated += 1
                details.append({"email": email_lower, "action": "reactivated", "sub_id": row["stripe_subscription_id"], "source": row["source"]})

    return {
        "status": "preview" if preview else "ok",
        "batch_id": batch_id if not preview else None,
        "reactivated": reactivated, "skipped": skipped,
        "total_emails": len(emails), "details": details,
        "next_step": "Set preview: false to execute" if preview else f"Revert with POST /api/admin/revert-batch batch_id={batch_id}"
    }


@app.post("/api/admin/ymove-deactivate")
async def ymove_deactivate(request: Request):
    """Deactivate subs confirmed expired by ymove API verification (S19).
    Body: {"emails": [...], "preview": true/false}
    Only deactivates Apple/Google source subs marked active."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = await request.json()
    emails = body.get("emails", [])
    if not emails:
        return JSONResponse(status_code=400, content={"error": "Provide emails list"})

    batch_id = body.get("batch_id", f"ymove_deact_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
    preview = body.get("preview", True)

    async with db_pool.acquire() as conn:
        deactivated = 0
        skipped = 0
        details = []
        for email in emails:
            email_lower = email.strip().lower()
            row = await conn.fetchrow("""
                SELECT id, stripe_subscription_id, source, status, plan_amount
                FROM subscriptions
                WHERE lower(email) = $1 AND status IN ('active', 'trialing') AND source IN ('apple', 'google')
                ORDER BY created_at DESC LIMIT 1
            """, email_lower)
            if not row:
                skipped += 1
                details.append({"email": email_lower, "action": "skipped", "reason": "no active apple/google sub"})
                continue
            if preview:
                details.append({"email": email_lower, "action": "would_deactivate", "sub_id": row["stripe_subscription_id"], "source": row["source"], "plan_amount": row["plan_amount"]})
                deactivated += 1
            else:
                await conn.execute("""
                    UPDATE subscriptions
                    SET status = 'canceled', canceled_at = NOW(), updated_at = NOW(), import_batch = $1
                    WHERE id = $2
                """, batch_id, row["id"])
                deactivated += 1
                details.append({"email": email_lower, "action": "deactivated", "sub_id": row["stripe_subscription_id"], "source": row["source"]})

    return {
        "status": "preview" if preview else "ok",
        "batch_id": batch_id if not preview else None,
        "deactivated": deactivated, "skipped": skipped,
        "total_emails": len(emails), "details": details,
        "next_step": "Set preview: false to execute" if preview else f"Revert with POST /api/admin/revert-batch batch_id={batch_id}"
    }


# --- Session 20: ymove Shadow Sync ---

_active_sync_task = None  # Track running sync task


@app.post("/api/admin/ymove-shadow-sync")
async def ymove_shadow_sync(request: Request):
    """Shadow sync: pull ymove data, compute diff, optionally apply.
    Actions:
      run    - Start background sync task
      status - Check progress of running/latest sync
      diff   - Get categorized diff from latest completed sync
      apply  - Apply diff changes with batch_id (preview/confirm)
    """
    global _active_sync_task
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")
    if not YMOVE_API_KEY:
        raise HTTPException(status_code=500, detail="YMOVE_API_KEY not configured")

    body = await request.json()
    action = body.get("action", "status")

    if action == "run":
        # Check for already running sync
        async with db_pool.acquire() as conn:
            running = await conn.fetchrow(
                "SELECT id, started_at FROM ymove_sync_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"
            )
            if running:
                age_minutes = (datetime.now(timezone.utc) - running["started_at"]).total_seconds() / 60
                # Auto-expire stuck runs after 30 minutes
                if age_minutes > 30:
                    await conn.execute(
                        "UPDATE ymove_sync_runs SET status = 'failed', error = 'Timed out after 30 minutes', completed_at = NOW() WHERE id = $1",
                        running["id"]
                    )
                else:
                    return {"status": "already_running", "run_id": running["id"],
                            "running_for_minutes": round(age_minutes, 1),
                            "hint": "Use action: status to check progress"}

            # Create new run record
            run_id = await conn.fetchval(
                "INSERT INTO ymove_sync_runs (status, phase) VALUES ('running', 'init') RETURNING id"
            )

        # Launch background task
        _active_sync_task = asyncio.create_task(_run_shadow_sync(run_id))
        return {"status": "started", "run_id": run_id, "hint": "Poll with action: status (every 10-15s)"}

    elif action == "status":
        async with db_pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM ymove_sync_runs ORDER BY started_at DESC LIMIT 1"
            )
        if not run:
            return {"status": "no_runs", "hint": "Start with action: run"}

        pct = ""
        if run["progress_total"] and run["progress_total"] > 0:
            pct = f" ({round(run['progress_current'] / run['progress_total'] * 100)}%)"

        result = {
            "run_id": run["id"],
            "status": run["status"],
            "phase": run["phase"],
            "progress": f"{run['progress_current']}/{run['progress_total']}{pct}" if run["progress_total"] else "initializing",
            "started_at": str(run["started_at"]),
            "completed_at": str(run["completed_at"]) if run["completed_at"] else None,
            "our_active_count": run["our_active_count"],
            "ymove_active_count": run["ymove_active_count"],
        }
        if run["error"]:
            result["error"] = run["error"]
        # Include summary if completed
        if run["status"] == "completed" and run["results"]:
            res_data = run["results"] if isinstance(run["results"], dict) else json.loads(run["results"])
            result["summary"] = {
                "to_deactivate": len(res_data.get("to_deactivate", [])),
                "to_reactivate": len(res_data.get("to_reactivate", [])),
                "cross_platform_switchers": len(res_data.get("cross_platform_switchers", [])),
                "active_stripe_in_ymove": res_data.get("active_stripe_in_ymove", 0),
                "truly_new": len(res_data.get("truly_new", [])),
                "unchanged": res_data.get("unchanged", 0),
                "not_found": res_data.get("not_found_in_ymove", 0),
                "pull_all_status": res_data.get("pull_all_status", "unknown"),
            }
        return result

    elif action == "diff":
        async with db_pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM ymove_sync_runs WHERE status = 'completed' ORDER BY started_at DESC LIMIT 1"
            )
        if not run:
            return {"status": "no_completed_runs", "hint": "Start with action: run, wait for completion"}

        res_data = run["results"] if isinstance(run["results"], dict) else json.loads(run["results"])
        return {
            "run_id": run["id"],
            "completed_at": str(run["completed_at"]),
            "our_active_count": run["our_active_count"],
            "ymove_active_count": run["ymove_active_count"],
            "to_deactivate": res_data.get("to_deactivate", []),
            "to_reactivate": res_data.get("to_reactivate", []),
            "cross_platform_switchers": res_data.get("cross_platform_switchers", []),
            "active_stripe_in_ymove": res_data.get("active_stripe_in_ymove", 0),
            "truly_new": res_data.get("truly_new", []),
            "unchanged": res_data.get("unchanged", 0),
            "not_found_in_ymove": res_data.get("not_found_in_ymove", 0),
            "errors": res_data.get("errors", 0),
            "pull_all_status": res_data.get("pull_all_status", "unknown"),
            "pull_all_emails_found": res_data.get("pull_all_emails_found", 0),
            "verified_count": res_data.get("verified_count", 0),
        }

    elif action == "apply":
        preview = body.get("preview", True)
        async with db_pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM ymove_sync_runs WHERE status = 'completed' ORDER BY started_at DESC LIMIT 1"
            )
        if not run:
            return {"status": "no_completed_runs", "hint": "Run a sync first"}

        res_data = run["results"] if isinstance(run["results"], dict) else json.loads(run["results"])
        return await _apply_shadow_sync(res_data, preview, run["id"])

    else:
        return JSONResponse(status_code=400, content={
            "error": f"Unknown action: {action}. Use run, status, diff, or apply."
        })


async def _run_shadow_sync(run_id: int):
    """Background task: verify active Apple/Google emails + best-effort pull_all from ymove."""
    try:
        print(f"[Shadow Sync] Run {run_id} starting...")

        # --- Gather our DB state ---
        async with db_pool.acquire() as conn:
            # All our active Apple/Google subs with email
            our_active = await conn.fetch(
                """SELECT id, lower(email) as email, source, stripe_subscription_id, plan_amount, plan_interval
                   FROM subscriptions
                   WHERE status IN ('active', 'trialing') AND source IN ('apple', 'google')
                   AND email != '' AND email IS NOT NULL
                   ORDER BY email"""
            )
            # All cancelled Apple/Google subs (most recent per email, for reactivation)
            our_cancelled_ag = await conn.fetch(
                """SELECT DISTINCT ON (lower(email))
                   id, lower(email) as email, source, stripe_subscription_id
                   FROM subscriptions
                   WHERE status = 'canceled' AND source IN ('apple', 'google')
                   AND email != '' AND email IS NOT NULL
                   ORDER BY lower(email), created_at DESC"""
            )
            # All known emails across all sources/statuses
            all_known_rows = await conn.fetch(
                "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND email IS NOT NULL"
            )

        our_active_emails = [r["email"] for r in our_active]
        our_active_set = set(our_active_emails)
        our_active_lookup = {r["email"]: r for r in our_active}
        our_cancelled_ag_map = {r["email"]: r for r in our_cancelled_ag if r["email"] not in our_active_set}
        all_known_emails = set(r["em"] for r in all_known_rows)

        total_to_verify = len(our_active_emails)
        print(f"[Shadow Sync] {total_to_verify} active Apple/Google emails to verify, {len(our_cancelled_ag_map)} cancelled AG candidates")

        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE ymove_sync_runs SET phase = 'verify_existing', progress_total = $1, our_active_count = $2 WHERE id = $3",
                total_to_verify, total_to_verify, run_id
            )

        # --- Phase 1: Verify all active Apple/Google emails against ymove ---
        verify_results = {}
        batch_size = 10
        delay_seconds = 1.5
        processed = 0

        async with httpx.AsyncClient(timeout=15.0) as client:
            for i, email in enumerate(our_active_emails):
                if i > 0 and i % batch_size == 0:
                    await asyncio.sleep(delay_seconds)
                try:
                    resp = await client.get(
                        f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                        headers={"X-Authorization": YMOVE_API_KEY},
                        params={"email": email}
                    )
                    if resp.status_code == 429:
                        retry_after = float(resp.headers.get("retry-after", "5"))
                        print(f"[Shadow Sync] Rate limited at {email}, backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        resp = await client.get(
                            f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                            headers={"X-Authorization": YMOVE_API_KEY},
                            params={"email": email}
                        )
                    if resp.status_code == 200:
                        verify_results[email] = _ymove_parse_status(resp.json())
                    elif resp.status_code == 404:
                        verify_results[email] = "not_found"
                    else:
                        verify_results[email] = "error"
                except Exception as e:
                    verify_results[email] = "error"
                    print(f"[Shadow Sync] Verify error for {email}: {e}")

                processed += 1
                if processed % 50 == 0:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE ymove_sync_runs SET progress_current = $1 WHERE id = $2",
                            processed, run_id
                        )

        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE ymove_sync_runs SET progress_current = $1, phase = 'pull_all' WHERE id = $2",
                processed, run_id
            )

        print(f"[Shadow Sync] Phase 1 done: verified {processed} emails")

        # --- Phase 2: Best-effort pull all subscribed members ---
        ymove_all_emails = set()
        pull_all_status = "starting"
        pull_all_pages = 0
        total_pages_est = 0

        try:
            page = 1
            async with httpx.AsyncClient(timeout=20.0) as client:
                while True:
                    try:
                        resp = await client.get(
                            f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup/all",
                            headers={"X-Authorization": YMOVE_API_KEY},
                            params={"status": "subscribed", "page": str(page)}
                        )
                    except httpx.TimeoutException:
                        print(f"[Shadow Sync] Timeout on pull_all page {page}")
                        pull_all_status = f"timeout_page_{page}"
                        break

                    if resp.status_code == 429:
                        retry_after = float(resp.headers.get("retry-after", "5"))
                        print(f"[Shadow Sync] Rate limited on pull_all page {page}, backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status_code != 200:
                        pull_all_status = f"error_page_{page}_http_{resp.status_code}"
                        print(f"[Shadow Sync] pull_all error: HTTP {resp.status_code} on page {page}")
                        break

                    data = resp.json()
                    users = data.get("users", [])
                    for u in users:
                        em = (u.get("email") or "").strip().lower()
                        if em:
                            ymove_all_emails.add(em)

                    total_pages_est = data.get("totalPages", 1)
                    pull_all_pages = page

                    if page >= total_pages_est:
                        pull_all_status = "success"
                        break

                    page += 1
                    await asyncio.sleep(1.0)

                    # Update progress every 50 pages
                    if page % 50 == 0:
                        async with db_pool.acquire() as conn:
                            await conn.execute(
                                "UPDATE ymove_sync_runs SET phase = $1 WHERE id = $2",
                                f"pull_all_page_{page}_of_{total_pages_est}", run_id
                            )
        except Exception as e:
            pull_all_status = f"error: {str(e)[:200]}"
            print(f"[Shadow Sync] pull_all exception: {e}")

        print(f"[Shadow Sync] Phase 2 done: pull_all_status={pull_all_status}, found {len(ymove_all_emails)} emails across {pull_all_pages} pages")

        # --- Phase 3: Compute diff ---
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE ymove_sync_runs SET phase = 'computing_diff', ymove_active_count = $1 WHERE id = $2",
                len(ymove_all_emails), run_id
            )
            # Query active Stripe emails to separate from cross-platform switchers
            active_stripe_rows = await conn.fetch(
                "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status IN ('active', 'trialing') AND source = 'stripe'"
            )
        active_stripe_emails = set(r["em"] for r in active_stripe_rows)

        to_deactivate = []
        to_reactivate = []
        cross_platform_switchers = []  # Cancelled Stripe, now active on Apple/Google
        active_stripe_in_ymove = 0     # Active Stripe users ymove also tracks (no action)
        truly_new = []
        unchanged = 0
        not_found = 0
        verify_errors = 0

        # Categorize our active subs based on ymove verification
        for email, ymove_status in verify_results.items():
            if ymove_status == "active":
                unchanged += 1
            elif ymove_status == "expired":
                r = our_active_lookup.get(email)
                if r:
                    to_deactivate.append({
                        "email": email,
                        "sub_id": r["stripe_subscription_id"],
                        "source": r["source"],
                        "plan_amount": r["plan_amount"],
                        "db_id": r["id"],
                    })
            elif ymove_status == "not_found":
                not_found += 1
            else:
                verify_errors += 1

        # Categorize ymove subscribers we don't have as active
        if pull_all_status == "success":
            for email in ymove_all_emails:
                if email in our_active_set:
                    continue
                if email in our_cancelled_ag_map:
                    row = our_cancelled_ag_map[email]
                    to_reactivate.append({
                        "email": email,
                        "sub_id": row["stripe_subscription_id"],
                        "source": row["source"],
                        "db_id": row["id"],
                    })
                elif email in active_stripe_emails:
                    # Active Stripe sub, ymove just tracks them too. No action needed.
                    active_stripe_in_ymove += 1
                elif email in all_known_emails:
                    # Known email but no active sub of any kind. Real cross-platform switcher.
                    cross_platform_switchers.append({"email": email})
                else:
                    truly_new.append({"email": email})

        # Build final results
        results = {
            "to_deactivate": to_deactivate,
            "to_reactivate": to_reactivate,
            "cross_platform_switchers": cross_platform_switchers,
            "active_stripe_in_ymove": active_stripe_in_ymove,
            "truly_new": truly_new,
            "unchanged": unchanged,
            "not_found_in_ymove": not_found,
            "errors": verify_errors,
            "pull_all_status": pull_all_status,
            "pull_all_pages": pull_all_pages,
            "pull_all_total_pages": total_pages_est,
            "pull_all_emails_found": len(ymove_all_emails),
            "verified_count": len(verify_results),
        }

        async with db_pool.acquire() as conn:
            await conn.execute(
                """UPDATE ymove_sync_runs SET
                   status = 'completed', phase = 'done', completed_at = NOW(),
                   progress_current = $1, results = $2,
                   ymove_active_count = $3
                   WHERE id = $4""",
                processed, json.dumps(results, default=str),
                len(ymove_all_emails), run_id
            )

        print(f"[Shadow Sync] Run {run_id} COMPLETED. "
              f"Deactivate: {len(to_deactivate)}, Reactivate: {len(to_reactivate)}, "
              f"Switchers: {len(cross_platform_switchers)}, Stripe-in-ymove: {active_stripe_in_ymove}, "
              f"New: {len(truly_new)}, Unchanged: {unchanged}, Not found: {not_found}")

    except Exception as e:
        print(f"[Shadow Sync] Run {run_id} FAILED: {e}")
        import traceback
        traceback.print_exc()
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE ymove_sync_runs SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2",
                    str(e)[:500], run_id
                )
        except Exception:
            pass


async def _apply_shadow_sync(results: dict, preview: bool, run_id: int) -> dict:
    """Apply shadow sync diff: deactivate expired, reactivate confirmed active."""
    batch_id = f"shadow_{run_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    to_deactivate = results.get("to_deactivate", [])
    to_reactivate = results.get("to_reactivate", [])
    cross_platform_switchers = results.get("cross_platform_switchers", [])
    active_stripe_in_ymove = results.get("active_stripe_in_ymove", 0)
    truly_new = results.get("truly_new", [])

    if preview:
        deact_mrr = sum(r.get("plan_amount", 1999) for r in to_deactivate)
        react_mrr = len(to_reactivate) * 1999
        return {
            "status": "preview",
            "batch_id": batch_id,
            "would_deactivate": len(to_deactivate),
            "would_reactivate": len(to_reactivate),
            "cross_platform_switchers": len(cross_platform_switchers),
            "active_stripe_in_ymove": active_stripe_in_ymove,
            "truly_new_found": len(truly_new),
            "mrr_impact_deactivate_cents": -deact_mrr,
            "mrr_impact_reactivate_cents": react_mrr,
            "net_mrr_impact_cents": react_mrr - deact_mrr,
            "deactivate_sample": to_deactivate[:25],
            "reactivate_sample": to_reactivate[:25],
            "switcher_sample": cross_platform_switchers[:25],
            "truly_new_sample": truly_new[:25],
            "note": "Send preview: false to execute deactivations and reactivations. Switchers and new subscribers are NOT auto-applied (require manual review/import)."
        }

    deactivated = 0
    reactivated = 0
    errors = 0

    async with db_pool.acquire() as conn:
        for item in to_deactivate:
            try:
                result = await conn.execute(
                    """UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(),
                       updated_at = NOW(), import_batch = $1
                       WHERE id = $2 AND status IN ('active', 'trialing')""",
                    batch_id, item["db_id"]
                )
                if result and result.endswith("1"):
                    deactivated += 1
            except Exception as e:
                print(f"[Shadow Sync Apply] Deactivate error {item.get('email')}: {e}")
                errors += 1

        for item in to_reactivate:
            try:
                result = await conn.execute(
                    """UPDATE subscriptions SET status = 'active', canceled_at = NULL,
                       updated_at = NOW(), import_batch = $1
                       WHERE id = $2 AND status = 'canceled'""",
                    batch_id, item["db_id"]
                )
                if result and result.endswith("1"):
                    reactivated += 1
            except Exception as e:
                print(f"[Shadow Sync Apply] Reactivate error {item.get('email')}: {e}")
                errors += 1

    return {
        "status": "applied",
        "batch_id": batch_id,
        "deactivated": deactivated,
        "reactivated": reactivated,
        "cross_platform_switchers": len(cross_platform_switchers),
        "active_stripe_in_ymove": active_stripe_in_ymove,
        "truly_new_found": len(truly_new),
        "errors": errors,
        "revert": f"POST /api/admin/revert-batch with batch_id={batch_id}",
        "note": f"Applied {deactivated} deactivations, {reactivated} reactivations. "
                f"{len(cross_platform_switchers)} switchers and {len(truly_new)} new subscribers NOT auto-applied (need manual review)."
    }


@app.post("/api/admin/ymove-import-new")
async def ymove_import_new(request: Request):
    """Import new Apple/Google subscribers discovered by shadow sync.
    Takes emails, verifies each against ymove to get provider, creates records.
    Body: {"emails": [...], "preview": true/false, "skip_test_accounts": true}
    """
    import hashlib
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")
    if not YMOVE_API_KEY:
        raise HTTPException(status_code=500, detail="YMOVE_API_KEY not configured")

    body = await request.json()
    emails = body.get("emails", [])
    if not emails:
        return JSONResponse(status_code=400, content={"error": "Provide emails list"})

    preview = body.get("preview", True)
    skip_test = body.get("skip_test_accounts", True)

    # Filter test accounts
    test_domains = ["ymove.app"]
    test_patterns = ["test", "dsfg", "asdf", "qwer"]
    filtered_emails = []
    skipped_test = []
    for email in emails:
        em = email.strip().lower()
        if not em or "@" not in em:
            continue
        domain = em.split("@")[-1]
        local = em.split("@")[0]
        is_test = False
        if skip_test:
            if domain in test_domains:
                is_test = True
            for pat in test_patterns:
                if pat in local:
                    is_test = True
                    break
        if is_test:
            skipped_test.append(em)
        else:
            filtered_emails.append(em)

    # Check which emails already have active Apple/Google subs (skip those)
    async with db_pool.acquire() as conn:
        active_ag_rows = await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status IN ('active', 'trialing') AND source IN ('apple', 'google')"
        )
    active_ag_emails = set(r["em"] for r in active_ag_rows)

    new_emails = [e for e in filtered_emails if e not in active_ag_emails]
    already_known = [e for e in filtered_emails if e in active_ag_emails]

    # Verify against ymove to get provider and confirm active
    verified = []
    not_active = []
    verify_errors = []
    batch_size = 10
    delay_seconds = 1.5

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i, email in enumerate(new_emails):
            if i > 0 and i % batch_size == 0:
                await asyncio.sleep(delay_seconds)
            try:
                resp = await client.get(
                    f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                    headers={"X-Authorization": YMOVE_API_KEY},
                    params={"email": email}
                )
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("retry-after", "5"))
                    await asyncio.sleep(retry_after)
                    resp = await client.get(
                        f"{YMOVE_API_BASE}/api/site/{YMOVE_SITE_ID}/member-lookup",
                        headers={"X-Authorization": YMOVE_API_KEY},
                        params={"email": email}
                    )
                if resp.status_code == 200:
                    data = resp.json()
                    status = _ymove_parse_status(data)
                    if status == "active":
                        # Try to determine provider from response
                        user_data = data.get("user", {})
                        provider = (user_data.get("subscriptionProvider") or
                                    user_data.get("provider") or "apple").lower()
                        if provider not in ("apple", "google"):
                            provider = "apple"
                        verified.append({"email": email, "provider": provider, "raw": user_data})
                    else:
                        not_active.append({"email": email, "status": status})
                else:
                    verify_errors.append({"email": email, "http_status": resp.status_code})
            except Exception as e:
                verify_errors.append({"email": email, "error": str(e)})

    if preview:
        by_provider = {}
        for v in verified:
            p = v["provider"]
            by_provider[p] = by_provider.get(p, 0) + 1
        return {
            "status": "preview",
            "total_input": len(emails),
            "skipped_test_accounts": len(skipped_test),
            "already_active_apple_google": len(already_known),
            "new_to_verify": len(new_emails),
            "verified_active": len(verified),
            "not_active": len(not_active),
            "verify_errors": len(verify_errors),
            "by_provider": by_provider,
            "would_import": len(verified),
            "est_mrr_impact_cents": len(verified) * 1999,
            "verified_sample": [{"email": v["email"], "provider": v["provider"]} for v in verified[:30]],
            "skipped_test_sample": skipped_test[:10],
            "not_active_sample": not_active[:10],
            "already_known_sample": already_known[:10],
            "note": "Send preview: false to create Apple/Google records for verified-active emails."
        }

    # Create records
    batch_id = f"ymove_import_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    imported = 0
    errors = 0

    async with db_pool.acquire() as conn:
        for v in verified:
            email = v["email"]
            provider = v["provider"]
            email_hash = hashlib.md5(email.encode()).hexdigest()[:16]
            syn_id = f"ymove_new_{provider}_{email_hash}"
            try:
                await conn.execute("""
                    INSERT INTO subscriptions (
                        stripe_customer_id, stripe_subscription_id, email, status,
                        plan_interval, plan_amount, currency, source,
                        created_at, updated_at, import_batch
                    ) VALUES ('', $1, $2, 'active', 'month', 1999, 'usd', $3, NOW(), NOW(), $4)
                    ON CONFLICT (stripe_subscription_id) DO NOTHING
                """, syn_id, email, provider, batch_id)
                imported += 1
            except Exception as e:
                print(f"[ymove import] Error for {email}: {e}")
                errors += 1

    return {
        "status": "ok",
        "batch_id": batch_id,
        "imported": imported,
        "errors": errors,
        "skipped_test_accounts": len(skipped_test),
        "already_active_apple_google": len(already_known),
        "not_active_in_ymove": len(not_active),
        "est_mrr_added_cents": imported * 1999,
        "revert": f"POST /api/admin/revert-batch with batch_id={batch_id}"
    }


async def run_daily_shadow_sync():
    """Automated daily shadow sync: verify all Apple/Google against ymove, auto-apply safe changes."""
    print(f"[Daily Sync] Starting automated shadow sync at {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M ET')}")
    try:
        if not db_pool or not YMOVE_API_KEY:
            print("[Daily Sync] Skipped: missing db_pool or YMOVE_API_KEY")
            return

        # Check for already running sync
        async with db_pool.acquire() as conn:
            running = await conn.fetchrow(
                "SELECT id, started_at FROM ymove_sync_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"
            )
            if running:
                age_minutes = (datetime.now(timezone.utc) - running["started_at"]).total_seconds() / 60
                if age_minutes > 30:
                    await conn.execute(
                        "UPDATE ymove_sync_runs SET status = 'failed', error = 'Timed out after 30 minutes', completed_at = NOW() WHERE id = $1",
                        running["id"]
                    )
                else:
                    print(f"[Daily Sync] Skipped: sync already running (id={running['id']}, {round(age_minutes,1)}m)")
                    return

            # Create run record
            run_id = await conn.fetchval(
                "INSERT INTO ymove_sync_runs (status, phase) VALUES ('running', 'init') RETURNING id"
            )

        # Run the sync (same function as manual trigger)
        await _run_shadow_sync(run_id)

        # Wait briefly for completion to be written
        await asyncio.sleep(2)

        # Auto-apply safe changes (deactivations + reactivations only)
        async with db_pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM ymove_sync_runs WHERE id = $1", run_id
            )

        if run and run["status"] == "completed" and run["results"]:
            res_data = run["results"] if isinstance(run["results"], dict) else json.loads(run["results"])
            to_deactivate = res_data.get("to_deactivate", [])
            to_reactivate = res_data.get("to_reactivate", [])
            switchers = len(res_data.get("cross_platform_switchers", []))
            truly_new = len(res_data.get("truly_new", []))

            if to_deactivate or to_reactivate:
                apply_result = await _apply_shadow_sync(res_data, False, run_id)
                print(f"[Daily Sync] Auto-applied: {apply_result.get('deactivated', 0)} deactivated, "
                      f"{apply_result.get('reactivated', 0)} reactivated, "
                      f"batch_id={apply_result.get('batch_id', 'n/a')}")
            else:
                print("[Daily Sync] No changes to apply. Database is in sync.")

            if switchers > 0 or truly_new > 0:
                print(f"[Daily Sync] Note: {switchers} cross-platform switchers and {truly_new} truly new subs found but NOT auto-imported. Review manually.")
        else:
            status = run["status"] if run else "unknown"
            error = run.get("error", "") if run else ""
            print(f"[Daily Sync] Sync did not complete successfully. Status: {status}, Error: {error}")

    except Exception as e:
        print(f"[Daily Sync] Error: {e}")
        import traceback
        traceback.print_exc()


@app.post("/api/admin/send-test-digest")
async def send_test_digest(request: Request):
    """Manually trigger a daily digest email (for testing)."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not RESEND_API_KEY:
        raise HTTPException(status_code=500, detail="RESEND_API_KEY not configured")
    if not DIGEST_RECIPIENTS:
        raise HTTPException(status_code=500, detail="DIGEST_RECIPIENTS not configured")

    stats = await gather_daily_stats()
    if "error" in stats:
        raise HTTPException(status_code=500, detail=stats["error"])

    insights = await generate_digest_insights(stats)

    now_et = datetime.now(ZoneInfo("America/New_York"))
    subject = f"[TEST] M&M Daily Digest - {now_et.strftime('%b %d')} | {stats.get('conversions_today',0)} conversions, {stats.get('new_subscriptions',0)} trials, {stats.get('cancellations_paid',0)} paid cancels, {stats.get('cancellations_trial',0)} trial cancels"

    html = build_digest_html(stats, insights)
    result = await send_digest_email(html, subject)

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return {"status": "sent", "recipients": result["recipients"], "stats_summary": {
        "new_subs": stats.get("new_subscriptions", 0),
        "cancellations": stats.get("cancellations", 0),
        "new_leads": stats.get("new_leads", 0),
        "gross_mrr": stats.get("gross_mrr", "$0"),
        "net_mrr": stats.get("net_mrr", "$0"),
    }}


# --- Admin Data Tools ---


@app.post("/api/admin/backfill-stripe")
async def backfill_stripe(request: Request):
    """Pull all Stripe subscriptions and upsert into DB."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    count = 0
    errors = 0
    try:
        subs_iter = stripe.Subscription.list(
            limit=100, status="all", expand=["data.customer"]
        )
        for sub in subs_iter.auto_paging_iter():
            try:
                sub_id = sub.id
                cust_obj = sub.get("customer") if isinstance(sub, dict) else sub.customer
                customer_id = cust_obj.get("id", str(cust_obj)) if isinstance(cust_obj, dict) else (cust_obj.id if hasattr(cust_obj, "id") else str(cust_obj))
                status = sub.status

                plan_amount = 0
                plan_interval = ""
                try:
                    items_data = sub["items"]["data"]
                    if items_data:
                        price = items_data[0]["price"]
                        plan_amount = price.get("unit_amount", 0) or 0
                        recurring = price.get("recurring")
                        if recurring:
                            plan_interval = recurring.get("interval", "") or ""
                except Exception:
                    pass

                email = ""
                try:
                    cust = sub.get("customer")
                    if isinstance(cust, dict):
                        email = cust.get("email", "") or ""
                    elif hasattr(cust, "email"):
                        email = cust.email or ""
                except Exception:
                    pass

                def ts(v):
                    if v:
                        return datetime.fromtimestamp(v, tz=timezone.utc)
                    return None

                real_created = ts(sub.created) if hasattr(sub, 'created') else None

                async with db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO subscriptions (
                            stripe_customer_id, stripe_subscription_id, email, status,
                            plan_interval, plan_amount, currency, source,
                            trial_start, trial_end,
                            current_period_start, current_period_end,
                            canceled_at, created_at, updated_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,'stripe',$8,$9,$10,$11,$12,COALESCE($13,NOW()),NOW())
                        ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            plan_interval = EXCLUDED.plan_interval,
                            plan_amount = EXCLUDED.plan_amount,
                            email = EXCLUDED.email,
                            trial_start = EXCLUDED.trial_start,
                            trial_end = EXCLUDED.trial_end,
                            current_period_start = EXCLUDED.current_period_start,
                            current_period_end = EXCLUDED.current_period_end,
                            canceled_at = EXCLUDED.canceled_at,
                            created_at = EXCLUDED.created_at,
                            updated_at = NOW()
                    """,
                        customer_id, sub_id, email, status,
                        plan_interval, plan_amount, sub.currency or "usd",
                        ts(sub.trial_start), ts(sub.trial_end),
                        ts(sub.current_period_start), ts(sub.current_period_end),
                        ts(sub.canceled_at), real_created
                    )
                count += 1
            except Exception as e:
                print(f"Backfill sub error: {e}")
                errors += 1
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe API error: {str(e)}")

    return {"status": "ok", "imported": count, "errors": errors}


@app.post("/api/admin/sync-trialing")
async def sync_trialing(request: Request):
    """Sync all Stripe subs stuck as 'trialing' with Stripe's real current status.
    Fixes zombie records from before the webhook was connected."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    async with db_pool.acquire() as conn:
        zombies = await conn.fetch(
            "SELECT id, stripe_subscription_id FROM subscriptions WHERE status = 'trialing' AND source = 'stripe'"
        )

    results = {"total_checked": len(zombies), "now_active": 0, "now_canceled": 0, "now_past_due": 0, "still_trialing": 0, "not_found": 0, "errors": 0}

    for z in zombies:
        sub_id = z["stripe_subscription_id"]
        if not sub_id or not sub_id.startswith("sub_"):
            results["not_found"] += 1
            continue
        try:
            real_sub = stripe.Subscription.retrieve(sub_id)
            real_status = real_sub.status
            if real_status == "trialing":
                results["still_trialing"] += 1
                continue

            def ts(v):
                if v:
                    return datetime.fromtimestamp(v, tz=timezone.utc)
                return None

            async with db_pool.acquire() as conn:
                await conn.execute(
                    """UPDATE subscriptions SET
                       status = $1,
                       canceled_at = $2,
                       current_period_start = $3,
                       current_period_end = $4,
                       updated_at = NOW()
                       WHERE id = $5""",
                    real_status,
                    ts(real_sub.canceled_at),
                    ts(real_sub.current_period_start),
                    ts(real_sub.current_period_end),
                    z["id"]
                )

                # If now active, they converted from trial and we missed it
                if real_status == "active":
                    await conn.execute(
                        "UPDATE subscriptions SET converted_at = COALESCE(trial_end, NOW()) WHERE id = $1 AND converted_at IS NULL",
                        z["id"]
                    )
                    results["now_active"] += 1
                elif real_status == "canceled":
                    results["now_canceled"] += 1
                elif real_status == "past_due":
                    results["now_past_due"] += 1
                else:
                    # incomplete, incomplete_expired, unpaid, paused
                    results["now_canceled"] += 1

        except stripe.error.InvalidRequestError:
            # Sub doesn't exist in Stripe anymore
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                    z["id"]
                )
            results["not_found"] += 1
        except Exception as e:
            print(f"Sync trialing error for {sub_id}: {e}")
            results["errors"] += 1

    return results


@app.post("/api/admin/fix-stripe-dates")
async def fix_stripe_dates(request: Request):
    """One-time fix: update created_at on all Stripe subs to use Stripe's real created timestamp.
    This corrects the backfill issue where all subs got created_at = NOW() instead of their real date."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    fixed = 0
    errors = 0
    skipped = 0
    try:
        subs_iter = stripe.Subscription.list(limit=100, status="all")
        for sub in subs_iter.auto_paging_iter():
            try:
                sub_id = sub.id
                stripe_created = sub.created
                if not stripe_created:
                    skipped += 1
                    continue
                real_dt = datetime.fromtimestamp(stripe_created, tz=timezone.utc)
                async with db_pool.acquire() as conn:
                    result = await conn.execute(
                        "UPDATE subscriptions SET created_at = $1 WHERE stripe_subscription_id = $2 AND source = 'stripe'",
                        real_dt, sub_id
                    )
                    if result and result.endswith('1'):
                        fixed += 1
                    else:
                        skipped += 1
            except Exception as e:
                print(f"Fix date error for {sub.id}: {e}")
                errors += 1
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe API error: {str(e)}")

    return {"status": "ok", "fixed": fixed, "skipped": skipped, "errors": errors}


@app.post("/api/admin/reset-data")
async def reset_data(request: Request):
    """Wipe all tables. Requires confirm=RESET in body."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    body = await request.json()
    if body.get("confirm") != "RESET":
        raise HTTPException(status_code=400, detail="Must send confirm=RESET")
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE leads, page_views, chat_sessions, subscriptions, subscription_events, ad_spend RESTART IDENTITY")

    return {"status": "ok", "message": "All data cleared"}


@app.post("/api/admin/import-leads-csv")
async def import_leads_csv(request: Request, file: UploadFile = File(...)):
    """Import leads from CSV file upload."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    import csv
    import io

    contents = await file.read()
    text = contents.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    def find_col(row, options):
        for opt in options:
            for key in row:
                if key.strip().lower() == opt.lower():
                    return (row[key] or "").strip()
        return ""

    async with db_pool.acquire() as conn:
        existing = await conn.fetch("SELECT DISTINCT lower(email) as em FROM leads WHERE email != ''")
        existing_emails = set(r["em"] for r in existing)

        imported = 0
        skipped = 0
        for row in reader:
            email = find_col(row, ["email", "e-mail"])
            if not email or email.lower() in existing_emails:
                skipped += 1
                continue
            first_name = find_col(row, ["first_name", "first", "firstname", "first name"])
            last_name = find_col(row, ["last_name", "last", "lastname", "last name"])
            referral = find_col(row, ["source", "referral_source", "referral", "how_heard"])

            await conn.execute(
                """INSERT INTO leads (first_name, email, extra, referral_source)
                   VALUES ($1, $2, $3, $4)""",
                first_name, email, last_name, referral
            )
            imported += 1
            existing_emails.add(email.lower())

    return {"status": "ok", "imported": imported, "skipped": skipped}


@app.post("/api/admin/import-subscribers-csv")
async def import_subscribers_csv(request: Request, file: UploadFile = File(...)):
    """Import Apple/Google subscribers from CSV into subscriptions table."""
    import csv as csv_mod
    import io as io_mod
    import hashlib

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    contents = await file.read()
    text = contents.decode("utf-8-sig")
    reader = csv_mod.DictReader(io_mod.StringIO(text))

    def find_col(row, options):
        for opt in options:
            for key in row:
                if key.strip().lower() == opt.lower():
                    return (row[key] or "").strip()
        return ""

    async with db_pool.acquire() as conn:
        existing = await conn.fetch("SELECT stripe_subscription_id FROM subscriptions")
        existing_ids = set(r["stripe_subscription_id"] for r in existing)

        imported = 0
        skipped = 0
        for row in reader:
            email = find_col(row, ["email", "e-mail"])
            if not email:
                skipped += 1
                continue

            source_raw = find_col(row, ["source"]).lower()
            if source_raw == "google":
                source = "google"
            else:
                source = "apple"

            email_hash = hashlib.md5(email.lower().encode()).hexdigest()[:16]
            syn_id = f"import_{source}_{email_hash}"

            if syn_id in existing_ids:
                skipped += 1
                continue

            date_str = find_col(row, ["date", "sign up date", "signup_date"])
            period_start = None
            if date_str:
                try:
                    period_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            plan_amount = 1999
            plan_interval = "month"

            try:
                await conn.execute("""
                    INSERT INTO subscriptions (
                        stripe_customer_id, stripe_subscription_id, email, status,
                        plan_interval, plan_amount, currency, source,
                        current_period_start, created_at, updated_at
                    ) VALUES ('', $1, $2, 'active', $3, $4, 'usd', $5, $6, COALESCE($7, NOW()), NOW())
                    ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                        email = EXCLUDED.email,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """,
                    syn_id, email, plan_interval, plan_amount, source, period_start, period_start
                )
                imported += 1
                existing_ids.add(syn_id)
            except Exception as e:
                print(f"Subscriber import error: {e}")
                skipped += 1

    return {"status": "ok", "imported": imported, "skipped": skipped}


@app.post("/api/admin/reconcile-cancellations")
async def reconcile_cancellations(request: Request, file: UploadFile = File(...)):
    """Upload CSV of cancelled emails. Matches against subscriptions and flips to canceled."""
    import csv as csv_mod
    import io as io_mod

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    contents = await file.read()
    text = contents.decode("utf-8-sig")
    reader = csv_mod.DictReader(io_mod.StringIO(text))

    cancelled_emails = set()
    for row in reader:
        for key in row:
            if key.strip().lower() in ("email", "e-mail"):
                val = (row[key] or "").strip().lower()
                if val and "@" in val:
                    cancelled_emails.add(val)

    if not cancelled_emails:
        return {"status": "ok", "matched": 0, "total_emails": 0}

    async with db_pool.acquire() as conn:
        active_subs = await conn.fetch(
            "SELECT id, email FROM subscriptions WHERE status IN ('active', 'trialing')"
        )
        matched = 0
        for sub in active_subs:
            if sub["email"] and sub["email"].strip().lower() in cancelled_emails:
                await conn.execute(
                    "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                    sub["id"]
                )
                matched += 1

    return {"status": "ok", "matched": matched, "total_emails": len(cancelled_emails)}


@app.post("/api/admin/backfill-trial-dates")
async def backfill_trial_dates(request: Request):
    """Sets trial_start/end on imported subs missing trial data."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        updated = await conn.execute(
            """UPDATE subscriptions
               SET trial_start = created_at,
                   trial_end = created_at + INTERVAL '30 days'
               WHERE trial_start IS NULL
               AND source IN ('apple', 'google')
               AND created_at IS NOT NULL"""
        )

    count = int(updated.split(" ")[-1]) if updated else 0
    return {"status": "ok", "updated": count}


# --- Session 11: Backfill converted_at for historical data ---

@app.post("/api/admin/backfill-conversions")
async def backfill_conversions(request: Request):
    """One-time backfill: stamp converted_at on subs that clearly converted from trial.
    Uses heuristic: had a trial (trial_end set) AND current_period_start moved past trial_end."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        # Stripe subs: trial_end exists and current_period moved past it
        stripe_result = await conn.execute(
            """UPDATE subscriptions
               SET converted_at = trial_end
               WHERE converted_at IS NULL
               AND trial_end IS NOT NULL
               AND current_period_start IS NOT NULL
               AND current_period_start > trial_end
               AND source = 'stripe'""")
        stripe_count = int(stripe_result.split(" ")[-1]) if stripe_result else 0

        # Apple/Google imported subs: if active and trial_end is set, they converted
        appgoogle_result = await conn.execute(
            """UPDATE subscriptions
               SET converted_at = trial_end
               WHERE converted_at IS NULL
               AND trial_end IS NOT NULL
               AND status = 'active'
               AND source IN ('apple', 'google')""")
        appgoogle_count = int(appgoogle_result.split(" ")[-1]) if appgoogle_result else 0

    return {
        "status": "ok",
        "stripe_backfilled": stripe_count,
        "apple_google_backfilled": appgoogle_count,
        "total": stripe_count + appgoogle_count,
    }


@app.post("/api/admin/backfill-readable-ids")
async def backfill_readable_ids(request: Request):
    """Assign persistent readable_id to all existing subscriptions that lack one."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        # Get all subs without readable_id, ordered by created_at so IDs are chronological
        rows = await conn.fetch(
            """SELECT id, stripe_subscription_id, source FROM subscriptions
               WHERE readable_id IS NULL
               ORDER BY source, created_at"""
        )

        # Get current max ID per source
        counters = {}
        for src in ("stripe", "apple", "google"):
            row = await conn.fetchrow(
                "SELECT readable_id FROM subscriptions WHERE source = $1 AND readable_id IS NOT NULL ORDER BY readable_id DESC LIMIT 1",
                src
            )
            if row and row["readable_id"]:
                try:
                    counters[src] = int(row["readable_id"].split("-")[-1])
                except (ValueError, IndexError):
                    counters[src] = 0
            else:
                counters[src] = 0

        assigned = 0
        for r in rows:
            src = r["source"] or "stripe"
            counters[src] = counters.get(src, 0) + 1
            rid = f"{src.upper()}-{counters[src]:04d}"
            await conn.execute(
                "UPDATE subscriptions SET readable_id = $1 WHERE id = $2",
                rid, r["id"]
            )
            assigned += 1

    return {
        "status": "ok",
        "assigned": assigned,
        "counters": {k: v for k, v in counters.items()},
    }


# --- Session 11: Subscriptions CSV Export ---

@app.get("/api/admin/subscriptions-csv")
async def admin_subscriptions_csv(request: Request):
    """Export all subscribers (Stripe + Apple + Google) as CSV with lead attribution."""
    import io as io_mod
    import csv as csv_mod

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.*,
                   l.first_name as lead_name,
                   l.utm_source as lead_utm_source,
                   l.utm_medium as lead_utm_medium,
                   l.utm_campaign as lead_utm_campaign,
                   l.created_at as lead_date
            FROM subscriptions s
            LEFT JOIN LATERAL (
                SELECT first_name, utm_source, utm_medium, utm_campaign, created_at
                FROM leads
                WHERE lower(leads.email) = lower(s.email) AND s.email != ''
                ORDER BY created_at DESC LIMIT 1
            ) l ON true
            ORDER BY s.created_at DESC
        """)

    # readable_id now comes from DB (assigned persistently)

    output = io_mod.StringIO()
    writer = csv_mod.writer(output)
    writer.writerow([
        "readable_id", "email", "name", "source", "status",
        "plan", "signup_date",
        "trial_start", "trial_end", "converted_at", "cancel_date",
        "months_active", "est_lifetime_revenue",
        "utm_source", "utm_medium", "utm_campaign",
        "email_known",
        "renewal_count",
        "last_renewed_at"
    ])

    for r in rows:
        source = r.get("source", "stripe") or "stripe"
        sub_id = r.get("stripe_subscription_id", "") or ""

        readable_id = r.get("readable_id", "") or sub_id or f"{source.upper()}-{r['id']}"

        email = r.get("email", "") or ""
        email_known = "TRUE" if email and "@" in email else "FALSE"

        plan_amount = r.get("plan_amount", 0) or 0
        plan_interval = r.get("plan_interval", "") or ""
        created = r.get("created_at")
        canceled = r.get("canceled_at")

        # Plan display
        if plan_interval == "year":
            plan_display = f"${plan_amount / 100:.2f}/yr"
        elif plan_interval == "month":
            plan_display = f"${plan_amount / 100:.2f}/mo"
        else:
            plan_display = f"${plan_amount / 100:.2f}" if plan_amount else "$0.00"

        # Months active
        months_active = 0
        est_revenue = 0
        if created and plan_amount:
            end_date = canceled if canceled else datetime.now(timezone.utc)
            months_active = round(max(1, (end_date - created).days / 30), 1)
            monthly_equiv = plan_amount / 12 if plan_interval == "year" else plan_amount
            est_revenue = round(monthly_equiv * months_active)

        writer.writerow([
            readable_id,
            email,
            r.get("lead_name", "") or "",
            source,
            r.get("status", ""),
            plan_display,
            str(created.date()) if created else "",
            str(r.get("trial_start", "").date() if r.get("trial_start") else ""),
            str(r.get("trial_end", "").date() if r.get("trial_end") else ""),
            str(r.get("converted_at", "").date() if r.get("converted_at") else ""),
            str(canceled.date()) if canceled else "",
            months_active,
            f"${est_revenue / 100:.2f}",
            r.get("lead_utm_source", "") or "",
            r.get("lead_utm_medium", "") or "",
            r.get("lead_utm_campaign", "") or "",
            email_known,
            r.get("renewal_count", 0) or 0,
            str(r.get("last_renewed_at", "").date() if r.get("last_renewed_at") else ""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mm-subscriptions.csv"}
    )



# --- Session 11: Multi-Tab XLSX Export (Meg format) ---

@app.get("/api/admin/subscriptions-xlsx")
async def admin_subscriptions_xlsx(request: Request):
    """Export subscribers as multi-tab XLSX matching Meg spreadsheet layout."""
    from io import BytesIO
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        all_subs = await conn.fetch("""
            SELECT s.*,
                   l.first_name as lead_name,
                   l.extra as lead_last_name,
                   l.utm_source as lead_utm_source,
                   l.utm_medium as lead_utm_medium,
                   l.utm_campaign as lead_utm_campaign
            FROM subscriptions s
            LEFT JOIN LATERAL (
                SELECT first_name, extra, utm_source, utm_medium, utm_campaign
                FROM leads
                WHERE lower(leads.email) = lower(s.email) AND s.email != ''
                ORDER BY created_at DESC LIMIT 1
            ) l ON true
            ORDER BY s.created_at
        """)

        offered_leads = await conn.fetch("""
            SELECT l.first_name, l.extra as last_name, l.email,
                   l.experience_level, l.goals, l.recommended_plan,
                   l.utm_source, l.utm_medium, l.utm_campaign, l.created_at
            FROM leads l
            WHERE l.email != ''
            AND NOT EXISTS (
                SELECT 1 FROM subscriptions s
                WHERE lower(s.email) = lower(l.email)
                AND s.status IN ('active', 'trialing')
            )
            AND lower(l.email) NOT IN (
                SELECT DISTINCT lower(email) FROM subscriptions
                WHERE email != '' AND status = 'canceled'
            )
            ORDER BY l.created_at DESC
        """)

    apple_google_active = []
    stripe_active = []
    trialing = []
    cancelled = []

    for r in all_subs:
        status = r.get("status", "")
        source = r.get("source", "stripe") or "stripe"
        if status == "trialing":
            trialing.append(r)
        elif status == "canceled":
            cancelled.append(r)
        elif status == "active":
            if source in ("apple", "google"):
                apple_google_active.append(r)
            else:
                stripe_active.append(r)

    wb = Workbook()

    hdr_font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
    hdr_fill_navy = PatternFill("solid", fgColor="182241")
    hdr_fill_green = PatternFill("solid", fgColor="2d6a2d")
    hdr_fill_blue = PatternFill("solid", fgColor="3949ab")
    hdr_fill_orange = PatternFill("solid", fgColor="b35a00")
    hdr_fill_red = PatternFill("solid", fgColor="c0392b")
    data_font = Font(name="Arial", size=10)
    center = Alignment(horizontal="center")
    thin_border = Border(bottom=Side(style="thin", color="E0E0E0"))

    SUB_HEADERS = [
        "Readable ID", "First Name", "Last Name", "Email", "Sign-up Date",
        "Source", "Status", "Plan", "Trial Start", "Trial End",
        "Converted", "Renewals", "Last Renewed", "Cancel Date",
        "Months Active", "Est Revenue", "UTM Source", "UTM Medium", "UTM Campaign"
    ]

    def sub_row(r):
        source = r.get("source", "stripe") or "stripe"
        rid = r.get("readable_id", "") or r.get("stripe_subscription_id", "") or ""
        plan_amount = r.get("plan_amount", 0) or 0
        plan_interval = r.get("plan_interval", "") or ""
        created = r.get("created_at")
        canceled = r.get("canceled_at")
        if plan_interval == "year":
            plan_disp = f"${plan_amount/100:.2f}/yr"
        elif plan_interval == "month":
            plan_disp = f"${plan_amount/100:.2f}/mo"
        else:
            plan_disp = f"${plan_amount/100:.2f}" if plan_amount else ""
        months = 0
        revenue = 0
        if created and plan_amount:
            end = canceled if canceled else datetime.now(timezone.utc)
            months = round(max(1, (end - created).days / 30), 1)
            meq = plan_amount / 12 if plan_interval == "year" else plan_amount
            revenue = round(meq * months)
        return [
            rid,
            r.get("lead_name", "") or "",
            r.get("lead_last_name", "") or "",
            r.get("email", "") or "",
            created.strftime("%Y-%m-%d") if created else "",
            source.upper(),
            r.get("status", ""),
            plan_disp,
            r["trial_start"].strftime("%Y-%m-%d") if r.get("trial_start") else "",
            r["trial_end"].strftime("%Y-%m-%d") if r.get("trial_end") else "",
            r["converted_at"].strftime("%Y-%m-%d") if r.get("converted_at") else "",
            r.get("renewal_count", 0) or 0,
            r["last_renewed_at"].strftime("%Y-%m-%d") if r.get("last_renewed_at") else "",
            canceled.strftime("%Y-%m-%d") if canceled else "",
            months,
            f"${revenue/100:.2f}" if revenue else "",
            r.get("lead_utm_source", "") or "",
            r.get("lead_utm_medium", "") or "",
            r.get("lead_utm_campaign", "") or "",
        ]

    def write_sub_sheet(ws, rows, hdr_fill):
        for ci, h in enumerate(SUB_HEADERS, 1):
            c = ws.cell(row=1, column=ci, value=h)
            c.font = hdr_font
            c.fill = hdr_fill
            c.alignment = center
        for ri, r in enumerate(rows, 2):
            vals = sub_row(r)
            for ci, v in enumerate(vals, 1):
                c = ws.cell(row=ri, column=ci, value=v)
                c.font = data_font
                c.border = thin_border
        for ci in range(1, len(SUB_HEADERS) + 1):
            max_len = len(SUB_HEADERS[ci-1])
            for ri in range(2, min(len(rows)+2, 100)):
                val = ws.cell(row=ri, column=ci).value
                if val:
                    max_len = max(max_len, len(str(val)))
            ws.column_dimensions[get_column_letter(ci)].width = min(max_len + 3, 30)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions if len(rows) > 0 else "A1:S1"

    ws1 = wb.active
    ws1.title = "Apple & Google Members"
    write_sub_sheet(ws1, apple_google_active, hdr_fill_navy)

    ws2 = wb.create_sheet("Stripe Members")
    write_sub_sheet(ws2, stripe_active, hdr_fill_green)

    ws3 = wb.create_sheet("Free Month Trial")
    write_sub_sheet(ws3, trialing, hdr_fill_blue)

    ws4 = wb.create_sheet("Downloaded & Offered Trial")
    lead_headers = [
        "First Name", "Last Name", "Email", "Date",
        "Experience", "Goals", "Recommended Plan",
        "UTM Source", "UTM Medium", "UTM Campaign"
    ]
    for ci, h in enumerate(lead_headers, 1):
        c = ws4.cell(row=1, column=ci, value=h)
        c.font = hdr_font
        c.fill = hdr_fill_orange
        c.alignment = center
    for ri, r in enumerate(offered_leads, 2):
        vals = [
            r.get("first_name", "") or "",
            r.get("last_name", "") or "",
            r.get("email", "") or "",
            r["created_at"].strftime("%Y-%m-%d") if r.get("created_at") else "",
            r.get("experience_level", "") or "",
            r.get("goals", "") or "",
            r.get("recommended_plan", "") or "",
            r.get("utm_source", "") or "",
            r.get("utm_medium", "") or "",
            r.get("utm_campaign", "") or "",
        ]
        for ci, v in enumerate(vals, 1):
            c = ws4.cell(row=ri, column=ci, value=v)
            c.font = data_font
            c.border = thin_border
    for ci in range(1, len(lead_headers) + 1):
        max_len = len(lead_headers[ci-1])
        for ri in range(2, min(len(offered_leads)+2, 100)):
            val = ws4.cell(row=ri, column=ci).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws4.column_dimensions[get_column_letter(ci)].width = min(max_len + 3, 30)
    ws4.freeze_panes = "A2"
    if len(offered_leads) > 0:
        ws4.auto_filter.ref = ws4.dimensions

    ws5 = wb.create_sheet("Cancelled Subscription")
    write_sub_sheet(ws5, cancelled, hdr_fill_red)
    for ri in range(2, len(cancelled) + 2):
        for ci in range(1, len(SUB_HEADERS) + 1):
            ws5.cell(row=ri, column=ci).fill = PatternFill("solid", fgColor="FFE0E0")

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=mm-subscribers.xlsx"}
    )


# --- Session 17: Meg Format XLSX Export ---

@app.get("/api/admin/subscriptions-meg-xlsx")
async def admin_subscriptions_meg_xlsx(request: Request):
    """Export subscribers in Meg's exact spreadsheet format.
    Tab 1: Apple & Google Members (no headers) - first, last, email, date, source
    Tab 2: Stripe Members (no headers) - first, last, email, date
    Tab 3: Free Month Trial (headers) - First Name, Last Name, Email, Sign Up Date, Payment Type
    Tab 4: Downloaded & Offered Trial (headers) - First Name, Last Name, Email, Sign Up Date
    Tab 5: Cancelled Subscription (no headers) - first, last, email"""
    from io import BytesIO
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        all_subs = await conn.fetch("""
            SELECT s.*, s.first_name as sub_first, s.last_name as sub_last,
                   l.first_name as lead_name, l.extra as lead_last_name
            FROM subscriptions s
            LEFT JOIN LATERAL (
                SELECT first_name, extra FROM leads
                WHERE lower(leads.email) = lower(s.email) AND s.email != ''
                ORDER BY created_at DESC LIMIT 1
            ) l ON true
            ORDER BY s.created_at
        """)
        offered_leads = await conn.fetch("""
            SELECT l.first_name, l.extra as last_name, l.email, l.created_at
            FROM leads l
            WHERE l.email != ''
            AND NOT EXISTS (
                SELECT 1 FROM subscriptions s
                WHERE lower(s.email) = lower(l.email)
                AND s.status IN ('active', 'trialing')
            )
            AND lower(l.email) NOT IN (
                SELECT DISTINCT lower(email) FROM subscriptions
                WHERE email != '' AND status = 'canceled'
            )
            ORDER BY l.created_at DESC
        """)

    apple_google_active = []
    stripe_active = []
    trialing = []
    cancelled = []
    for r in all_subs:
        status = r.get("status", "")
        source = r.get("source", "stripe") or "stripe"
        if status == "trialing":
            trialing.append(r)
        elif status == "canceled":
            cancelled.append(r)
        elif status == "active":
            if source in ("apple", "google"):
                apple_google_active.append(r)
            else:
                stripe_active.append(r)

    wb = Workbook()
    hdr_font = Font(name="Arial", bold=True, size=11)
    data_font = Font(name="Arial", size=10)

    def get_name(r):
        first = r.get("sub_first", "") or r.get("lead_name", "") or ""
        last = r.get("sub_last", "") or r.get("lead_last_name", "") or ""
        return first, last

    def auto_width(ws, num_cols, num_rows):
        for ci in range(1, num_cols + 1):
            max_len = 10
            for ri in range(1, min(num_rows + 2, 100)):
                val = ws.cell(row=ri, column=ci).value
                if val:
                    max_len = max(max_len, len(str(val)))
            ws.column_dimensions[get_column_letter(ci)].width = min(max_len + 2, 30)

    # Tab 1: Apple & Google Members (NO headers, matching Meg format)
    ws1 = wb.active
    ws1.title = "Apple & Google Members"
    for ri, r in enumerate(apple_google_active, 1):
        first, last = get_name(r)
        created = r.get("created_at")
        source = (r.get("source", "") or "").upper()
        ws1.cell(row=ri, column=1, value=first).font = data_font
        ws1.cell(row=ri, column=2, value=last).font = data_font
        ws1.cell(row=ri, column=3, value=r.get("email", "") or "").font = data_font
        ws1.cell(row=ri, column=4, value=created.strftime("%Y-%m-%d") if created else "").font = data_font
        ws1.cell(row=ri, column=5, value=source).font = data_font
    auto_width(ws1, 5, len(apple_google_active))

    # Tab 2: Stripe Members (NO headers)
    ws2 = wb.create_sheet("Stripe Members")
    for ri, r in enumerate(stripe_active, 1):
        first, last = get_name(r)
        created = r.get("created_at")
        ws2.cell(row=ri, column=1, value=first).font = data_font
        ws2.cell(row=ri, column=2, value=last).font = data_font
        ws2.cell(row=ri, column=3, value=r.get("email", "") or "").font = data_font
        ws2.cell(row=ri, column=4, value=created.strftime("%Y-%m-%d") if created else "").font = data_font
    auto_width(ws2, 4, len(stripe_active))

    # Tab 3: Free Month Trial (WITH headers)
    ws3 = wb.create_sheet("Free month trial")
    for ci, h in enumerate(["First Name", "Last Name", "Email", "Sign Up Date", "Payment Type"], 1):
        ws3.cell(row=1, column=ci, value=h).font = hdr_font
    for ri, r in enumerate(trialing, 2):
        first, last = get_name(r)
        created = r.get("created_at")
        source = (r.get("source", "stripe") or "stripe").capitalize()
        ws3.cell(row=ri, column=1, value=first).font = data_font
        ws3.cell(row=ri, column=2, value=last).font = data_font
        ws3.cell(row=ri, column=3, value=r.get("email", "") or "").font = data_font
        ws3.cell(row=ri, column=4, value=created.strftime("%Y-%m-%d") if created else "").font = data_font
        ws3.cell(row=ri, column=5, value=source).font = data_font
    auto_width(ws3, 5, len(trialing) + 1)
    ws3.freeze_panes = "A2"

    # Tab 4: Downloaded & Offered Trial (WITH headers)
    ws4 = wb.create_sheet("downloaded, offered free trial")
    for ci, h in enumerate(["First Name", "Last Name", "Email", "Sign Up Date"], 1):
        ws4.cell(row=1, column=ci, value=h).font = hdr_font
    for ri, r in enumerate(offered_leads, 2):
        ws4.cell(row=ri, column=1, value=r.get("first_name", "") or "").font = data_font
        ws4.cell(row=ri, column=2, value=r.get("last_name", "") or "").font = data_font
        ws4.cell(row=ri, column=3, value=r.get("email", "") or "").font = data_font
        created = r.get("created_at")
        ws4.cell(row=ri, column=4, value=created.strftime("%Y-%m-%d") if created else "").font = data_font
    auto_width(ws4, 4, len(offered_leads) + 1)
    ws4.freeze_panes = "A2"

    # Tab 5: Cancelled Subscription (NO headers)
    # S18: Filter out rows with no email (cosmetic fix)
    cancelled_with_email = [r for r in cancelled if r.get("email")]
    ws5 = wb.create_sheet("cancelled subscription")
    for ri, r in enumerate(cancelled_with_email, 1):
        first, last = get_name(r)
        ws5.cell(row=ri, column=1, value=first).font = data_font
        ws5.cell(row=ri, column=2, value=last).font = data_font
        ws5.cell(row=ri, column=3, value=r.get("email", "") or "").font = data_font
    auto_width(ws5, 3, len(cancelled_with_email))

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=mm-meg-format.xlsx"}
    )


# --- Session 11: User Journey CSV (lead -> subscriber timeline) ---

@app.get("/api/admin/user-journey-csv")
async def admin_user_journey_csv(request: Request):
    """Export lead-to-subscriber journey: joins leads + subscriptions by email."""
    import io as io_mod
    import csv as csv_mod

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                l.email,
                l.first_name,
                l.created_at as lead_date,
                l.utm_source, l.utm_medium, l.utm_campaign,
                l.experience_level, l.goals, l.recommended_plan,
                s.source as sub_source,
                s.status as sub_status,
                s.plan_interval, s.plan_amount,
                s.trial_start, s.trial_end, s.converted_at,
                s.created_at as sub_date,
                s.canceled_at,
                s.current_period_start, s.current_period_end
            FROM leads l
            LEFT JOIN LATERAL (
                SELECT * FROM subscriptions
                WHERE lower(subscriptions.email) = lower(l.email) AND l.email != ''
                ORDER BY created_at DESC LIMIT 1
            ) s ON true
            WHERE l.email != ''
            ORDER BY l.created_at DESC
        """)

    output = io_mod.StringIO()
    writer = csv_mod.writer(output)
    writer.writerow([
        "email", "first_name",
        "lead_date", "utm_source", "utm_medium", "utm_campaign",
        "experience_level", "goals", "recommended_plan",
        "subscribed", "sub_source", "sub_status", "sub_date",
        "plan_interval", "plan_amount",
        "trial_start", "trial_end", "converted_at", "canceled_at",
        "days_lead_to_sub", "est_lifetime_revenue"
    ])

    for r in rows:
        subscribed = "yes" if r.get("sub_source") else "no"

        days_to_sub = ""
        if r.get("lead_date") and r.get("sub_date"):
            delta = r["sub_date"] - r["lead_date"]
            days_to_sub = max(0, delta.days)

        plan_amount = r.get("plan_amount", 0) or 0
        plan_interval = r.get("plan_interval", "") or ""
        sub_date = r.get("sub_date")
        canceled = r.get("canceled_at")
        est_revenue = 0
        if sub_date and plan_amount:
            end_date = canceled if canceled else datetime.now(timezone.utc)
            months_active = max(1, (end_date - sub_date).days / 30)
            monthly_equiv = plan_amount / 12 if plan_interval == "year" else plan_amount
            est_revenue = round(monthly_equiv * months_active)

        writer.writerow([
            r.get("email", ""),
            r.get("first_name", ""),
            str(r.get("lead_date", "") or ""),
            r.get("utm_source", "") or "",
            r.get("utm_medium", "") or "",
            r.get("utm_campaign", "") or "",
            r.get("experience_level", "") or "",
            r.get("goals", "") or "",
            r.get("recommended_plan", "") or "",
            subscribed,
            r.get("sub_source", "") or "",
            r.get("sub_status", "") or "",
            str(r.get("sub_date", "") or ""),
            plan_interval,
            f"${plan_amount / 100:.2f}" if plan_amount else "$0.00",
            str(r.get("trial_start", "") or ""),
            str(r.get("trial_end", "") or ""),
            str(r.get("converted_at", "") or ""),
            str(canceled or ""),
            str(days_to_sub),
            f"${est_revenue / 100:.2f}",
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mm-user-journey.csv"}
    )


# --- Session 11: Ad Spend CRUD ---

@app.post("/api/admin/ad-spend")
async def save_ad_spend(req: AdSpendRequest, request: Request):
    """Save or update ad spend for a month/channel combination."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    if not req.month or not req.channel:
        raise HTTPException(status_code=400, detail="month and channel required")

    amount_cents = round(req.amount_dollars * 100)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO ad_spend (month, channel, amount_cents, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (month, channel) DO UPDATE SET
                amount_cents = EXCLUDED.amount_cents,
                updated_at = NOW()
        """, req.month, req.channel.lower().strip(), amount_cents)

    return {"status": "ok", "month": req.month, "channel": req.channel, "amount_cents": amount_cents}


@app.get("/api/admin/ad-spend")
async def get_ad_spend(request: Request):
    """Retrieve all ad spend records."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM ad_spend ORDER BY month DESC, channel")

    return {"spend": [
        {"month": r["month"], "channel": r["channel"], "amount_cents": r["amount_cents"]}
        for r in rows
    ]}


@app.delete("/api/admin/ad-spend")
async def delete_ad_spend(request: Request):
    """Delete a specific ad spend entry."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    body = await request.json()
    month = body.get("month", "")
    channel = body.get("channel", "")
    if not month or not channel:
        raise HTTPException(status_code=400, detail="month and channel required")

    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM ad_spend WHERE month = $1 AND channel = $2", month, channel)

    return {"status": "ok"}


# --- Session 11: Attach Email + Search (consolidated) ---

@app.post("/api/admin/attach-email")
async def attach_email(request: Request):
    """Permanently attach an email to an Apple/Google subscription.
    Once attached, leads-table matching gives full name + UTM attribution."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    body = await request.json()
    sub_id = (body.get("subscription_id") or "").strip()
    email = (body.get("email") or "").strip().lower()

    if not sub_id or not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Valid subscription_id and email required")

    async with db_pool.acquire() as conn:
        # Find the subscription
        row = await conn.fetchrow(
            "SELECT id, source, email as current_email FROM subscriptions WHERE stripe_subscription_id = $1",
            sub_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"Subscription '{sub_id}' not found")

        await conn.execute(
            "UPDATE subscriptions SET email = $1, updated_at = NOW() WHERE stripe_subscription_id = $2",
            email, sub_id
        )

        # Check if we can match to a lead now
        lead = await conn.fetchrow(
            "SELECT first_name, utm_source, utm_medium, utm_campaign FROM leads WHERE lower(email) = $1 ORDER BY created_at DESC LIMIT 1",
            email
        )

    result = {
        "status": "ok",
        "subscription_id": sub_id,
        "email": email,
        "source": row["source"],
        "previous_email": row["current_email"] or "(none)",
    }
    if lead:
        result["matched_lead"] = {
            "name": lead["first_name"] or "",
            "utm_source": lead["utm_source"] or "",
            "utm_medium": lead["utm_medium"] or "",
            "utm_campaign": lead["utm_campaign"] or "",
        }
    else:
        result["matched_lead"] = None

    return result


@app.get("/api/admin/search-subscriptions")
async def search_subscriptions(request: Request):
    """Search subscriptions by ID prefix or email for the attach-email UI."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    q = request.query_params.get("q", "").strip()
    if len(q) < 2:
        return {"results": []}

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT stripe_subscription_id, email, source, status, plan_amount, plan_interval
            FROM subscriptions
            WHERE stripe_subscription_id ILIKE $1 OR email ILIKE $1
            ORDER BY created_at DESC LIMIT 10
        """, f"%{q}%")

    return {"results": [
        {
            "id": r["stripe_subscription_id"],
            "email": r["email"] or "",
            "source": r["source"],
            "status": r["status"],
            "plan": f"${(r['plan_amount'] or 0)/100:.2f}/{r['plan_interval'] or '?'}",
        }
        for r in rows
    ]}


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not db_pool:
        return {"error": "No database connected"}

    # Date range filtering (optional query params)
    date_from_str = request.query_params.get("from", "")
    date_to_str = request.query_params.get("to", "")
    date_from_dt = None
    date_to_dt = None
    try:
        if date_from_str:
            date_from_dt = datetime.strptime(date_from_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if date_to_str:
            date_to_dt = datetime.strptime(date_to_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    except Exception as e:
        print(f"Date parse error: {e}")

    async with db_pool.acquire() as conn:
        # Leads (with optional date filter)
        if date_from_dt or date_to_dt:
            lead_count_q = "SELECT COUNT(*) FROM leads WHERE 1=1"
            lead_list_q = "SELECT * FROM leads WHERE 1=1"
            idx = 1
            args = []
            if date_from_dt:
                lead_count_q += " AND created_at >= $" + str(idx)
                lead_list_q += " AND created_at >= $" + str(idx)
                args.append(date_from_dt)
                idx += 1
            if date_to_dt:
                lead_count_q += " AND created_at < $" + str(idx)
                lead_list_q += " AND created_at < $" + str(idx)
                args.append(date_to_dt)
                idx += 1
            lead_list_q += " ORDER BY created_at DESC LIMIT 50"
            total_leads = await conn.fetchval(lead_count_q, *args)
            recent_leads = await conn.fetch(lead_list_q, *args)
        else:
            total_leads = await conn.fetchval("SELECT COUNT(*) FROM leads")
            recent_leads = await conn.fetch(
                "SELECT * FROM leads ORDER BY created_at DESC LIMIT 50"
            )

        # Page views (with optional date filter)
        if date_from_dt or date_to_dt:
            pv_q = "SELECT COUNT(*) FROM page_views WHERE 1=1"
            pv_args = []
            pv_idx = 1
            if date_from_dt:
                pv_q += " AND created_at >= $" + str(pv_idx)
                pv_args.append(date_from_dt)
                pv_idx += 1
            if date_to_dt:
                pv_q += " AND created_at < $" + str(pv_idx)
                pv_args.append(date_to_dt)
                pv_idx += 1
            total_views = await conn.fetchval(pv_q, *pv_args)
            today_views = await conn.fetchval(
                "SELECT COUNT(*) FROM page_views WHERE created_at::date = CURRENT_DATE"
            )
        else:
            total_views = await conn.fetchval("SELECT COUNT(*) FROM page_views")
            today_views = await conn.fetchval(
                "SELECT COUNT(*) FROM page_views WHERE created_at::date = CURRENT_DATE"
            )
        views_by_page = await conn.fetch(
            """SELECT page, COUNT(*) as views FROM page_views
               WHERE created_at > NOW() - INTERVAL '7 days'
               GROUP BY page ORDER BY views DESC"""
        )
        views_by_day = await conn.fetch(
            """SELECT created_at::date as day, COUNT(*) as views FROM page_views
               WHERE created_at > NOW() - INTERVAL '7 days'
               GROUP BY day ORDER BY day"""
        )

        # Chat sessions
        total_chats = await conn.fetchval("SELECT COUNT(*) FROM chat_sessions")
        chats_by_type = await conn.fetch(
            "SELECT session_type, COUNT(*) as count FROM chat_sessions GROUP BY session_type"
        )

        # Phase 5: UTM attribution
        leads_by_source = await conn.fetch(
            """SELECT COALESCE(NULLIF(utm_source,''), 'direct') as channel, COUNT(*) as count
               FROM leads GROUP BY channel ORDER BY count DESC"""
        )
        leads_by_medium = await conn.fetch(
            """SELECT COALESCE(NULLIF(utm_medium,''), 'none') as medium, COUNT(*) as count
               FROM leads GROUP BY medium ORDER BY count DESC"""
        )
        leads_by_campaign = await conn.fetch(
            """SELECT COALESCE(NULLIF(utm_campaign,''), 'none') as campaign, COUNT(*) as count
               FROM leads GROUP BY campaign ORDER BY count DESC LIMIT 10"""
        )

        # Subscription stats (Phase 3)
        active_subs = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE status IN ('active', 'trialing')"
        )
        trialing_subs = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE status = 'trialing'"
        )
        canceled_subs = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE status = 'canceled'"
        )
        total_subs = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

        # MRR calculation: sum of active monthly amounts
        # Monthly subs count as-is, annual subs divided by 12
        mrr_monthly = await conn.fetchval(
            """SELECT COALESCE(SUM(plan_amount), 0) FROM subscriptions
               WHERE status = 'active' AND plan_interval = 'month'"""
        )
        mrr_annual = await conn.fetchval(
            """SELECT COALESCE(SUM(plan_amount / 12), 0) FROM subscriptions
               WHERE status = 'active' AND plan_interval = 'year'"""
        )
        mrr_cents = (mrr_monthly or 0) + (mrr_annual or 0)

        # Session 11: Fixed trial -> paid conversion
        # "Ever converted" = has converted_at OR (had trial and current_period moved past trial_end)
        total_ever_trialed = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE trial_start IS NOT NULL"
        )
        converted_from_trial = await conn.fetchval(
            """SELECT COUNT(*) FROM subscriptions
               WHERE trial_start IS NOT NULL
               AND (converted_at IS NOT NULL
                    OR (trial_end IS NOT NULL AND current_period_start IS NOT NULL AND current_period_start > trial_end))"""
        )

        # Rolling cohort data for conversion windows
        cohort_subs = await conn.fetch(
            """SELECT created_at, converted_at, canceled_at, status, trial_start, trial_end, current_period_start
               FROM subscriptions WHERE trial_start IS NOT NULL"""
        )

        # Recent conversion activity
        conversions_7d = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE converted_at > NOW() - INTERVAL '7 days'"
        ) or 0
        conversions_30d = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE converted_at > NOW() - INTERVAL '30 days'"
        ) or 0

        # Churn: canceled in last 30 days vs active at start of period
        churned_30d = await conn.fetchval(
            """SELECT COUNT(*) FROM subscriptions
               WHERE status = 'canceled' AND canceled_at > NOW() - INTERVAL '30 days'"""
        )

        # Recent subscription events
        recent_events = await conn.fetch(
            """SELECT event_type, stripe_customer_id, source, created_at
               FROM subscription_events ORDER BY created_at DESC LIMIT 20"""
        )

        # Subscriptions by status
        subs_by_status = await conn.fetch(
            "SELECT status, COUNT(*) as count FROM subscriptions GROUP BY status ORDER BY count DESC"
        )

        # Phase 4: Subscriptions by source
        subs_by_source = await conn.fetch(
            "SELECT source, COUNT(*) as count, SUM(CASE WHEN status IN ('active','trialing') THEN 1 ELSE 0 END) as active_count FROM subscriptions GROUP BY source ORDER BY count DESC"
        )

        # MRR by source
        mrr_by_source = await conn.fetch(
            """SELECT source,
                COALESCE(SUM(CASE WHEN plan_interval='month' THEN plan_amount ELSE 0 END), 0) as mrr_monthly,
                COALESCE(SUM(CASE WHEN plan_interval='year' THEN plan_amount/12 ELSE 0 END), 0) as mrr_annual
               FROM subscriptions WHERE status = 'active'
               GROUP BY source"""
        )

        # Phase 5b: MRR trend (last 6 months)
        mrr_trend = await conn.fetch(
            """SELECT
                to_char(date_trunc('month', updated_at), 'YYYY-MM') as month,
                COALESCE(SUM(CASE WHEN plan_interval='month' THEN plan_amount ELSE 0 END), 0) as monthly_total,
                COALESCE(SUM(CASE WHEN plan_interval='year' THEN plan_amount/12 ELSE 0 END), 0) as annual_total
               FROM subscriptions
               WHERE status = 'active' AND updated_at > NOW() - INTERVAL '6 months'
               GROUP BY month ORDER BY month"""
        )

        # Avg subscriber lifetime (all who ever converted â includes churned for honest LTV)
        avg_sub_age = await conn.fetchval(
            """SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (COALESCE(canceled_at, NOW()) - created_at)) / 86400), 0)
               FROM subscriptions
               WHERE converted_at IS NOT NULL
               OR (trial_end IS NOT NULL AND current_period_start IS NOT NULL AND current_period_start > trial_end)"""
        )

        # Avg monthly revenue per converted sub (includes churned for honest LTV)
        avg_monthly_per_sub = await conn.fetchval(
            """SELECT COALESCE(AVG(
                CASE WHEN plan_interval='month' THEN plan_amount
                     WHEN plan_interval='year' THEN plan_amount/12
                     ELSE 0 END
               ), 0) FROM subscriptions
               WHERE converted_at IS NOT NULL
               OR (trial_end IS NOT NULL AND current_period_start IS NOT NULL AND current_period_start > trial_end)"""
        )

        # Total subscribers ever (for lifetime calc)
        total_subs_ever = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

        # ARR
        arr_cents = mrr_cents * 12

        # Session 11: Ad spend data
        ad_spend_rows = await conn.fetch("SELECT * FROM ad_spend ORDER BY month DESC, channel")

        # Session 15: Conversion funnel by UTM source
        funnel_rows = await conn.fetch("""
            SELECT
              COALESCE(NULLIF(l.utm_source, ''), 'unknown') as channel,
              COUNT(*)::int as trials,
              SUM(CASE WHEN s.converted_at IS NOT NULL
                   OR (s.trial_end IS NOT NULL AND s.current_period_start IS NOT NULL
                       AND s.current_period_start > s.trial_end)
                   THEN 1 ELSE 0 END)::int as converted,
              SUM(CASE WHEN NOT (s.converted_at IS NOT NULL
                   OR (s.trial_end IS NOT NULL AND s.current_period_start IS NOT NULL
                       AND s.current_period_start > s.trial_end))
                   AND s.status = 'trialing' AND s.trial_end IS NOT NULL AND s.trial_end > NOW()
                   THEN 1 ELSE 0 END)::int as still_trialing
            FROM subscriptions s
            LEFT JOIN LATERAL (
              SELECT utm_source FROM leads
              WHERE lower(leads.email) = lower(s.email) AND s.email != ''
              ORDER BY created_at DESC LIMIT 1
            ) l ON true
            WHERE s.trial_start IS NOT NULL
            GROUP BY channel
            ORDER BY trials DESC
        """)

        # Ad spend totals by channel (all months summed)
        ad_spend_totals = await conn.fetch(
            "SELECT channel, SUM(amount_cents)::int as total_cents FROM ad_spend GROUP BY channel"
        )

    # Build response
    trial_conversion_rate = 0
    if total_ever_trialed and total_ever_trialed > 0:
        trial_conversion_rate = round((converted_from_trial / total_ever_trialed) * 100, 1)

    # Rolling cohort conversion windows (7d, 30d, 60d, 90d, all)
    now_utc = datetime.now(timezone.utc)
    cohort_windows = [
        {"label": "7d", "days": 7},
        {"label": "30d", "days": 30},
        {"label": "60d", "days": 60},
        {"label": "90d", "days": 90},
        {"label": "all", "days": None},
    ]
    conversion_cohorts = []
    for w in cohort_windows:
        cutoff = now_utc - timedelta(days=w["days"]) if w["days"] else None
        trials = converted = canceled = still_trialing = mature = 0
        for s in cohort_subs:
            created = s["created_at"]
            if not created:
                continue
            if cutoff and created < cutoff:
                continue
            trials += 1
            if (now_utc - created).days >= 30:
                mature += 1
            has_converted = (s["converted_at"] is not None) or (s["trial_end"] and s["current_period_start"] and s["current_period_start"] > s["trial_end"])
            if has_converted:
                converted += 1
            elif s["status"] == "canceled":
                canceled += 1
            elif s["status"] == "trialing" and s["trial_end"] and s["trial_end"] > now_utc:
                still_trialing += 1
            else:
                canceled += 1
        decided = converted + canceled
        conversion_cohorts.append({
            "window": w["label"],
            "trials": trials,
            "converted": converted,
            "canceled": canceled,
            "still_trialing": still_trialing,
            "conversion_rate": round((converted / decided) * 100, 1) if decided > 0 else None,
            "maturity_pct": round((mature / trials) * 100, 1) if trials > 0 else 0,
        })

    churn_rate = 0
    if active_subs and active_subs > 0:
        churn_rate = round((churned_30d / (active_subs + churned_30d)) * 100, 1)

    # Session 15: Build conversion funnel data
    spend_by_channel = {r["channel"]: r["total_cents"] for r in ad_spend_totals}
    conversion_funnel = []
    all_trials = all_converted = all_canceled = all_trialing = 0
    all_spend = sum(spend_by_channel.values())

    for fr in funnel_rows:
        ch = fr["channel"]
        trials = fr["trials"]
        converted = fr["converted"]
        st = fr["still_trialing"]
        canceled = trials - converted - st
        decided = converted + canceled
        rate = round((converted / decided) * 100, 1) if decided > 0 else None
        spend = spend_by_channel.get(ch, 0)
        cpa = round(spend / converted) if converted > 0 and spend > 0 else None

        all_trials += trials
        all_converted += converted
        all_canceled += canceled
        all_trialing += st

        conversion_funnel.append({
            "channel": ch,
            "trials": trials,
            "converted": converted,
            "canceled": canceled,
            "still_trialing": st,
            "conv_rate": rate,
            "ad_spend_cents": spend,
            "cpa_cents": cpa,
        })

    all_decided = all_converted + all_canceled
    all_rate = round((all_converted / all_decided) * 100, 1) if all_decided > 0 else None
    all_cpa = round(all_spend / all_converted) if all_converted > 0 and all_spend > 0 else None
    conversion_funnel.insert(0, {
        "channel": "__all__",
        "trials": all_trials,
        "converted": all_converted,
        "canceled": all_canceled,
        "still_trialing": all_trialing,
        "conv_rate": all_rate,
        "ad_spend_cents": all_spend,
        "cpa_cents": all_cpa,
    })

    return {
        "leads": {
            "total": total_leads,
            "recent": [dict(r) for r in recent_leads],
            "by_source": [{"channel": r["channel"], "count": r["count"]} for r in leads_by_source],
            "by_medium": [{"medium": r["medium"], "count": r["count"]} for r in leads_by_medium],
            "by_campaign": [{"campaign": r["campaign"], "count": r["count"]} for r in leads_by_campaign],
        },
        "page_views": {
            "total": total_views,
            "today": today_views,
            "by_page": [{"page": r["page"], "views": r["views"]} for r in views_by_page],
            "by_day": [{"day": str(r["day"]), "views": r["views"]} for r in views_by_day],
        },
        "chats": {
            "total": total_chats,
            "by_type": [{"session_type": r["session_type"], "count": r["count"]} for r in chats_by_type],
        },
        "subscriptions": {
            "active": active_subs or 0,
            "trialing": trialing_subs or 0,
            "canceled": canceled_subs or 0,
            "total": total_subs or 0,
            "mrr_cents": mrr_cents,
            "mrr_display": f"${mrr_cents / 100:,.2f}",
            "trial_conversion_rate": trial_conversion_rate,
            "conversions_7d": conversions_7d,
            "conversions_30d": conversions_30d,
            "conversion_cohorts": conversion_cohorts,
            "churn_rate_30d": churn_rate,
            "churned_30d": churned_30d or 0,
            "by_status": [{"status": r["status"], "count": r["count"]} for r in subs_by_status],
            "by_source": [{"source": r["source"], "total": r["count"], "active": r["active_count"]} for r in subs_by_source],
            "mrr_by_source": [
                {"source": r["source"], "mrr_cents": (r["mrr_monthly"] or 0) + (r["mrr_annual"] or 0)}
                for r in mrr_by_source
            ],
            "mrr_trend": [
                {"month": r["month"], "mrr_cents": (r["monthly_total"] or 0) + (r["annual_total"] or 0)}
                for r in mrr_trend
            ],
            "arr_cents": arr_cents,
            "arr_display": f"${arr_cents / 100:,.2f}",
            "avg_sub_age_days": round(float(avg_sub_age or 0), 1),
            "avg_monthly_per_sub_cents": round(float(avg_monthly_per_sub or 0)),
            "est_ltv_cents": round(float(avg_monthly_per_sub or 0) * max(float(avg_sub_age or 30) / 30, 1)),
            "est_ltv_display": f"${round(float(avg_monthly_per_sub or 0) * max(float(avg_sub_age or 30) / 30, 1)) / 100:,.2f}",
            "recent_events": [
                {
                    "event_type": r["event_type"],
                    "customer": r["stripe_customer_id"],
                    "source": r.get("source", "stripe"),
                    "created_at": str(r["created_at"]),
                }
                for r in recent_events
            ],
        },
        "conversion_funnel": conversion_funnel,
        "ad_spend": [
            {"month": r["month"], "channel": r["channel"], "amount_cents": r["amount_cents"]}
            for r in ad_spend_rows
        ],
    }


@app.get("/api/admin/leads-csv")
async def admin_leads_csv(request: Request):
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM leads ORDER BY created_at DESC")

    import io
    import csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "first_name", "email", "experience_level", "goals", "referral_source", "recommended_plan", "extra", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "ym_source", "created_at"])
    for r in rows:
        writer.writerow([r["id"], r["first_name"], r["email"], r["experience_level"], r["goals"], r["referral_source"], r["recommended_plan"], r["extra"], r.get("utm_source",""), r.get("utm_medium",""), r.get("utm_campaign",""), r.get("utm_term",""), r.get("utm_content",""), r.get("ym_source",""), str(r["created_at"])])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mm-leads.csv"}
    )


@app.get("/api/health")
async def health():
    db_status = "connected" if db_pool else "not connected"
    stripe_status = "configured" if STRIPE_SECRET_KEY else "not configured"
    digest_status = "active" if (RESEND_API_KEY and DIGEST_RECIPIENTS and scheduler) else "disabled"
    shadow_sync_status = "active" if (YMOVE_API_KEY and scheduler) else "disabled"
    return {
        "status": "ok",
        "service": "Movement & Miles",
        "version": "22.0.0",
        "database": db_status,
        "stripe": stripe_status,
        "daily_digest": digest_status,
        "digest_recipients": DIGEST_RECIPIENTS if DIGEST_RECIPIENTS else "none",
        "daily_shadow_sync": shadow_sync_status,
        "shadow_sync_schedule": "8:00 AM ET daily" if shadow_sync_status == "active" else "disabled",
    }



# --- Session 16: Dedup Active Subscriptions ---

@app.post("/api/admin/dedup-active")
async def dedup_active(request: Request):
    """Find and resolve duplicate active subscriptions for the same email.
    Without confirm=true: preview. With confirm=true: cancel the inferior duplicate."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    confirm = body.get("confirm", False)

    async with db_pool.acquire() as conn:
        dups = await conn.fetch("""
            SELECT lower(email) as em,
                   array_agg(id ORDER BY updated_at DESC) as ids,
                   array_agg(source ORDER BY updated_at DESC) as sources,
                   array_agg(stripe_subscription_id ORDER BY updated_at DESC) as sub_ids,
                   array_agg(plan_amount ORDER BY updated_at DESC) as amounts
            FROM subscriptions
            WHERE status IN ('active', 'trialing') AND email != '' AND email IS NOT NULL
            GROUP BY lower(email) HAVING COUNT(*) > 1
        """)

        results = []
        canceled_count = 0
        for d in dups:
            # Pick keeper: prefer stripe (live webhooks), then most recently updated
            keep_idx = 0
            for i, src in enumerate(d["sources"]):
                if src == "stripe" and d["sources"][keep_idx] != "stripe":
                    keep_idx = i
                    break

            entries = []
            for i in range(len(d["ids"])):
                action = "KEEP" if i == keep_idx else "CANCEL"
                entries.append({"id": d["ids"][i], "sub_id": d["sub_ids"][i], "source": d["sources"][i], "amount": d["amounts"][i], "action": action})
                if confirm and action == "CANCEL":
                    await conn.execute(
                        "UPDATE subscriptions SET status = 'canceled', canceled_at = NOW(), updated_at = NOW() WHERE id = $1",
                        d["ids"][i]
                    )
                    canceled_count += 1
            results.append({"email": d["em"], "records": entries})

    if confirm:
        return {"status": "ok", "duplicates_found": len(dups), "canceled": canceled_count, "details": results}
    return {"status": "preview", "duplicates_found": len(dups), "plan": results}


# --- Session 16: Meg Apple/Google Import ---

@app.post("/api/admin/import-meg-apple-google")
async def import_meg_apple_google(request: Request, file: UploadFile = File(...)):
    """Import from Meg Apple & Google Members sheet (headerless: first,last,email,date,source).
    ?preview=true for dry run. All imports tagged with batch_id for revert."""
    import hashlib

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    preview_mode = request.query_params.get("preview", "false").lower() == "true"
    skip_reactivate = request.query_params.get("skip_reactivate", "false").lower() == "true"
    contents = await file.read()

    from openpyxl import load_workbook
    from io import BytesIO
    wb = load_workbook(BytesIO(contents), read_only=True, data_only=True)

    target_sheet = None
    for name in wb.sheetnames:
        if "apple" in name.lower() and "google" in name.lower():
            target_sheet = name
            break
    if not target_sheet:
        wb.close()
        raise HTTPException(status_code=400, detail=f"Apple & Google sheet not found. Sheets: {wb.sheetnames}")

    ws = wb[target_sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    parsed = []
    for r in rows:
        if not any(r):
            continue
        email = str(r[2] or "").strip().lower() if len(r) > 2 else ""
        if not email or "@" not in email:
            continue
        first_name = str(r[0] or "").strip() if len(r) > 0 else ""
        last_name = str(r[1] or "").strip() if len(r) > 1 else ""
        date_val = r[3] if len(r) > 3 else None
        source_val = str(r[4] or "").strip().lower() if len(r) > 4 else "apple"
        if source_val not in ("apple", "google"):
            source_val = "apple"
        period_start = None
        if hasattr(date_val, "strftime"):
            period_start = date_val.replace(tzinfo=timezone.utc) if date_val.tzinfo is None else date_val
        parsed.append({"first_name": first_name, "last_name": last_name, "email": email, "date": period_start, "source": source_val})

    batch_id = f"meg_ag_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    async with db_pool.acquire() as conn:
        active_emails = set(r["em"] for r in await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status IN ('active', 'trialing')"
        ))
        cancelled_emails = set(r["em"] for r in await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status = 'canceled'"
        ))
        all_emails = set(r["em"] for r in await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != ''"
        ))

        new_imports = []
        skip_active = []
        reactivate_candidates = []
        seen = set()

        for p in parsed:
            if p["email"] in seen:
                continue
            seen.add(p["email"])
            if p["email"] in active_emails:
                skip_active.append(p)
            elif p["email"] in cancelled_emails:
                reactivate_candidates.append(p)
            else:
                new_imports.append(p)

        if preview_mode:
            return {
                "status": "preview",
                "total_parsed": len(parsed),
                "unique_emails": len(seen),
                "would_import_new": len(new_imports),
                "would_skip_already_active": len(skip_active),
                "reactivate_candidates": len(reactivate_candidates),
                "note_reactivate": "These are in DB as cancelled but Meg says active. Only Apple/Google source subs will be reactivated (not Stripe).",
                "sample_new": [{"email": p["email"], "source": p["source"], "date": str(p["date"] or "")} for p in new_imports[:15]],
                "sample_reactivate": [{"email": p["email"], "source": p["source"]} for p in reactivate_candidates[:15]],
            }

        count_before = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")
        imported = 0
        reactivated = 0
        errors = 0

        for p in new_imports:
            email_hash = hashlib.md5(p["email"].encode()).hexdigest()[:16]
            syn_id = f"meg_{p['source']}_{email_hash}"
            try:
                await conn.execute("""
                    INSERT INTO subscriptions (
                        stripe_customer_id, stripe_subscription_id, email, status,
                        plan_interval, plan_amount, currency, source,
                        current_period_start, created_at, updated_at, import_batch,
                        first_name, last_name
                    ) VALUES ('', $1, $2, 'active', 'month', 1999, 'usd', $3, $4, COALESCE($5, NOW()), NOW(), $6, $7, $8)
                    ON CONFLICT (stripe_subscription_id) DO NOTHING
                """,
                    syn_id, p["email"], p["source"],
                    p["date"], p["date"], batch_id,
                    p.get("first_name", ""), p.get("last_name", "")
                )
                imported += 1
            except Exception as e:
                print(f"[Meg import] Error: {e}")
                errors += 1

        # Reactivate: only Apple/Google source subs (Stripe webhook is authoritative)
        if skip_reactivate:
            reactivate_candidates = []  # skip all reactivations
        for p in reactivate_candidates:
            try:
                result = await conn.execute("""
                    UPDATE subscriptions SET status = 'active', canceled_at = NULL,
                        updated_at = NOW(), import_batch = $1
                    WHERE id = (
                        SELECT id FROM subscriptions
                        WHERE lower(email) = $2 AND status = 'canceled' AND source IN ('apple', 'google')
                        ORDER BY created_at DESC LIMIT 1
                    )
                """, batch_id, p["email"])
                if result and result.endswith("1"):
                    reactivated += 1
            except Exception as e:
                print(f"[Meg import] Reactivate error: {e}")
                errors += 1

        count_after = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

    return {
        "status": "ok",
        "batch_id": batch_id,
        "imported_new": imported,
        "reactivated": reactivated,
        "skipped_already_active": len(skip_active),
        "errors": errors,
        "count_before": count_before,
        "count_after": count_after,
        "revert_info": f"To undo: POST /api/admin/revert-batch with batch_id={batch_id}. Note: revert deletes new imports and re-cancels reactivated subs."
    }


# --- Session 16: Export Reactivation Candidates ---

@app.post("/api/admin/reactivation-candidates")
async def reactivation_candidates(request: Request, file: UploadFile = File(...)):
    """Upload Meg Apple/Google XLSX, returns full list of reactivation candidates (cancelled in DB, active in Meg sheet)."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    contents = await file.read()
    from openpyxl import load_workbook
    from io import BytesIO
    wb = load_workbook(BytesIO(contents), read_only=True, data_only=True)

    target_sheet = None
    for name in wb.sheetnames:
        if "apple" in name.lower() and "google" in name.lower():
            target_sheet = name
            break
    if not target_sheet:
        wb.close()
        raise HTTPException(status_code=400, detail="Apple & Google sheet not found")

    ws = wb[target_sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    meg_people = {}
    for r in rows:
        if not any(r):
            continue
        email = str(r[2] or "").strip().lower() if len(r) > 2 else ""
        if not email or "@" not in email:
            continue
        first_name = str(r[0] or "").strip() if len(r) > 0 else ""
        last_name = str(r[1] or "").strip() if len(r) > 1 else ""
        source_val = str(r[4] or "").strip().upper() if len(r) > 4 else "APPLE"
        date_val = r[3].strftime("%Y-%m-%d") if len(r) > 3 and hasattr(r[3], "strftime") else ""
        meg_people[email] = {"first_name": first_name, "last_name": last_name, "source": source_val or "APPLE", "date": date_val}

    async with db_pool.acquire() as conn:
        active_emails = set(r["em"] for r in await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != '' AND status IN ('active', 'trialing')"
        ))
        cancelled_rows = await conn.fetch("""
            SELECT lower(email) as em, source, status, canceled_at, created_at
            FROM subscriptions
            WHERE email != '' AND status = 'canceled'
            AND lower(email) IN (SELECT unnest($1::text[]))
            ORDER BY email, created_at DESC
        """, list(meg_people.keys()))

    # Build cancelled lookup (most recent per email)
    cancelled_map = {}
    for r in cancelled_rows:
        if r["em"] not in cancelled_map:
            cancelled_map[r["em"]] = {"source": r["source"], "canceled_at": str(r["canceled_at"] or ""), "created_at": str(r["created_at"] or "")}

    candidates = []
    for email, info in meg_people.items():
        if email in active_emails:
            continue
        if email in cancelled_map:
            db_info = cancelled_map[email]
            candidates.append({
                "email": email,
                "first_name": info["first_name"],
                "last_name": info["last_name"],
                "meg_source": info["source"],
                "meg_date": info["date"],
                "db_source": db_info["source"],
                "db_canceled_at": db_info["canceled_at"],
                "db_created_at": db_info["created_at"],
            })

    candidates.sort(key=lambda x: x["email"])

    return {
        "total_meg_entries": len(meg_people),
        "already_active_in_db": len([e for e in meg_people if e in active_emails]),
        "reactivation_candidates": len(candidates),
        "candidates": candidates
    }


# --- Session 16: Comprehensive Data Audit ---

@app.get("/api/admin/data-audit")
async def data_audit(request: Request):
    """Run 15+ data quality checks across all tables. Returns issues that affect financial reports."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    audit = {"checks": {}, "summary": {}, "critical_issues": [], "warnings": []}

    async with db_pool.acquire() as conn:

        # ============================================================
        # CHECK 1: Overall counts
        # ============================================================
        total = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")
        active = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status = 'active'")
        trialing = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status = 'trialing'")
        canceled = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status = 'canceled'")
        other_status = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status NOT IN ('active', 'trialing', 'canceled')")
        audit["checks"]["totals"] = {"total": total, "active": active, "trialing": trialing, "canceled": canceled, "other_status": other_status}

        # ============================================================
        # CHECK 2: Subs with NO email (cannot attribute, cannot match leads)
        # ============================================================
        no_email = await conn.fetch(
            "SELECT source, status, COUNT(*) as cnt FROM subscriptions WHERE email IS NULL OR email = '' GROUP BY source, status ORDER BY cnt DESC"
        )
        no_email_total = sum(r["cnt"] for r in no_email)
        no_email_active = sum(r["cnt"] for r in no_email if r["status"] in ("active", "trialing"))
        audit["checks"]["no_email"] = {
            "total_without_email": no_email_total,
            "active_without_email": no_email_active,
            "by_source_status": [{"source": r["source"], "status": r["status"], "count": r["cnt"]} for r in no_email]
        }
        if no_email_active > 0:
            audit["critical_issues"].append(f"{no_email_active} active/trialing subs have NO email. Cannot attribute to leads, cannot match cancellations, invisible in funnel.")

        # ============================================================
        # CHECK 3: Duplicate active subs (same email, multiple active records = double MRR)
        # ============================================================
        dup_active = await conn.fetch("""
            SELECT lower(email) as em, COUNT(*) as cnt,
                   array_agg(source) as sources,
                   SUM(CASE WHEN plan_interval='month' THEN plan_amount ELSE plan_amount/12 END) as total_mrr_cents
            FROM subscriptions
            WHERE status IN ('active', 'trialing') AND email != '' AND email IS NOT NULL
            GROUP BY lower(email) HAVING COUNT(*) > 1
            ORDER BY cnt DESC LIMIT 50
        """)
        dup_mrr_inflate = sum(r["total_mrr_cents"] - (1999 if r["total_mrr_cents"] else 0) for r in dup_active)
        audit["checks"]["duplicate_active_emails"] = {
            "count": len(dup_active),
            "estimated_mrr_inflation_cents": dup_mrr_inflate,
            "top_duplicates": [{"email": r["em"], "active_records": r["cnt"], "sources": list(r["sources"]), "combined_mrr_cents": r["total_mrr_cents"]} for r in dup_active[:20]]
        }
        if len(dup_active) > 0:
            audit["critical_issues"].append(f"{len(dup_active)} emails have MULTIPLE active subscriptions. This inflates MRR by ~${dup_mrr_inflate/100:,.2f}. Each person should only have 1 active sub.")

        # ============================================================
        # CHECK 4: Plan amount anomalies (wrong amounts = wrong MRR)
        # ============================================================
        plan_dist = await conn.fetch("""
            SELECT plan_amount, plan_interval, source, status, COUNT(*) as cnt
            FROM subscriptions
            WHERE status IN ('active', 'trialing')
            GROUP BY plan_amount, plan_interval, source, status
            ORDER BY cnt DESC
        """)
        zero_amount_active = sum(r["cnt"] for r in plan_dist if (r["plan_amount"] or 0) == 0)
        weird_amounts = [r for r in plan_dist if r["plan_amount"] not in (0, 1999, 17999) and r["plan_amount"] is not None]
        audit["checks"]["plan_amounts"] = {
            "active_with_zero_amount": zero_amount_active,
            "distribution": [{"amount": r["plan_amount"], "interval": r["plan_interval"], "source": r["source"], "status": r["status"], "count": r["cnt"]} for r in plan_dist],
            "unexpected_amounts": [{"amount": r["plan_amount"], "interval": r["plan_interval"], "source": r["source"], "count": r["cnt"]} for r in weird_amounts]
        }
        if zero_amount_active > 0:
            audit["warnings"].append(f"{zero_amount_active} active subs have $0 plan_amount. They count as active but contribute nothing to MRR.")
        if weird_amounts:
            audit["warnings"].append(f"{len(weird_amounts)} plan_amount values are not standard ($19.99/mo or $179.99/yr). Could indicate data corruption.")

        # ============================================================
        # CHECK 5: Stale trialing subs (trial_end in the past but still trialing)
        # ============================================================
        stale_trials = await conn.fetchval("""
            SELECT COUNT(*) FROM subscriptions
            WHERE status = 'trialing' AND trial_end IS NOT NULL AND trial_end < NOW()
        """)
        audit["checks"]["stale_trialing"] = {"count": stale_trials or 0}
        if stale_trials and stale_trials > 0:
            audit["critical_issues"].append(f"{stale_trials} subs are still marked 'trialing' but their trial has ended. These inflate trial count and may be hiding cancellations or conversions.")

        # ============================================================
        # CHECK 6: Future dates (created_at or trial_start in the future)
        # ============================================================
        future_created = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE created_at > NOW() + INTERVAL '1 day'")
        future_trial = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE trial_start > NOW() + INTERVAL '1 day'")
        future_period = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE current_period_start > NOW() + INTERVAL '1 year'")
        audit["checks"]["future_dates"] = {
            "future_created_at": future_created or 0,
            "future_trial_start": future_trial or 0,
            "far_future_period_start": future_period or 0
        }
        if (future_created or 0) > 0:
            audit["critical_issues"].append(f"{future_created} subs have created_at in the future. Distorts daily metrics and cohort analysis.")

        # ============================================================
        # CHECK 7: Source integrity (source vs subscription_id pattern)
        # ============================================================
        stripe_no_sub = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'stripe' AND stripe_subscription_id NOT LIKE 'sub_%' AND stripe_subscription_id NOT LIKE 'import_%'")
        import_tagged = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE stripe_subscription_id LIKE 'import_%'")
        apple_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'apple'")
        google_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'google'")
        stripe_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'stripe'")
        audit["checks"]["source_integrity"] = {
            "stripe_total": stripe_count or 0,
            "apple_total": apple_count or 0,
            "google_total": google_count or 0,
            "import_tagged_records": import_tagged or 0,
            "stripe_without_sub_prefix": stripe_no_sub or 0
        }

        # ============================================================
        # CHECK 8: MRR accuracy (calculated vs what dashboard would show)
        # ============================================================
        mrr_monthly = await conn.fetchval("SELECT COALESCE(SUM(plan_amount), 0) FROM subscriptions WHERE status = 'active' AND plan_interval = 'month'")
        mrr_annual = await conn.fetchval("SELECT COALESCE(SUM(plan_amount / 12), 0) FROM subscriptions WHERE status = 'active' AND plan_interval = 'year'")
        mrr_total = (mrr_monthly or 0) + (mrr_annual or 0)
        mrr_by_src = await conn.fetch("""
            SELECT source,
                COUNT(*) as active_count,
                COALESCE(SUM(CASE WHEN plan_interval='month' THEN plan_amount ELSE 0 END), 0) as monthly,
                COALESCE(SUM(CASE WHEN plan_interval='year' THEN plan_amount/12 ELSE 0 END), 0) as annual_equiv
            FROM subscriptions WHERE status = 'active'
            GROUP BY source
        """)
        audit["checks"]["mrr_breakdown"] = {
            "total_mrr_cents": mrr_total,
            "total_mrr_display": f"${mrr_total/100:,.2f}",
            "from_monthly": mrr_monthly or 0,
            "from_annual": mrr_annual or 0,
            "by_source": [{"source": r["source"], "active_count": r["active_count"], "mrr_cents": (r["monthly"] or 0) + (r["annual_equiv"] or 0)} for r in mrr_by_src]
        }

        # ============================================================
        # CHECK 9: Missing conversion stamps
        # ============================================================
        should_have_converted = await conn.fetchval("""
            SELECT COUNT(*) FROM subscriptions
            WHERE converted_at IS NULL
            AND trial_end IS NOT NULL AND current_period_start IS NOT NULL
            AND current_period_start > trial_end
            AND status = 'active'
        """)
        has_converted = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE converted_at IS NOT NULL")
        trial_with_data = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE trial_start IS NOT NULL")
        trial_no_data = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE trial_start IS NULL")
        audit["checks"]["conversion_tracking"] = {
            "has_converted_at": has_converted or 0,
            "missing_converted_at_but_clearly_converted": should_have_converted or 0,
            "subs_with_trial_data": trial_with_data or 0,
            "subs_without_trial_data": trial_no_data or 0
        }
        if should_have_converted and should_have_converted > 10:
            audit["warnings"].append(f"{should_have_converted} active subs clearly converted from trial but lack converted_at stamp. Run backfill-conversions to fix.")

        # ============================================================
        # CHECK 10: Active subs that Meg lists as cancelled
        # (Cross-reference: how many of our "active" subs might actually be cancelled?)
        # ============================================================
        active_stripe_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'stripe' AND status = 'active'")
        active_apple_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'apple' AND status IN ('active', 'trialing')")
        active_google_count = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE source = 'google' AND status IN ('active', 'trialing')")
        audit["checks"]["active_by_source"] = {
            "stripe_active": active_stripe_count or 0,
            "apple_active": active_apple_count or 0,
            "google_active": active_google_count or 0,
            "note": "Compare these against Meg spreadsheet: 866 Stripe active, 796 Apple/Google active. Large discrepancies = stale data."
        }

        # ============================================================
        # CHECK 11: Subscription age analysis (are old subs still marked active?)
        # ============================================================
        old_active = await conn.fetch("""
            SELECT source, COUNT(*) as cnt,
                MIN(created_at) as oldest,
                MAX(created_at) as newest
            FROM subscriptions
            WHERE status = 'active'
            GROUP BY source
        """)
        very_old = await conn.fetchval("""
            SELECT COUNT(*) FROM subscriptions
            WHERE status = 'active' AND created_at < NOW() - INTERVAL '2 years'
        """)
        audit["checks"]["active_age"] = {
            "by_source": [{"source": r["source"], "active_count": r["cnt"], "oldest": str(r["oldest"]), "newest": str(r["newest"])} for r in old_active],
            "active_older_than_2_years": very_old or 0
        }
        if very_old and very_old > 50:
            audit["warnings"].append(f"{very_old} subs marked active are over 2 years old. Some may be zombies that cancelled outside our tracking.")

        # ============================================================
        # CHECK 12: period_end in the past for active subs (expired but not flipped)
        # ============================================================
        expired_active = await conn.fetchval("""
            SELECT COUNT(*) FROM subscriptions
            WHERE status = 'active'
            AND current_period_end IS NOT NULL
            AND current_period_end < NOW() - INTERVAL '7 days'
        """)
        audit["checks"]["expired_but_active"] = {"count": expired_active or 0}
        if expired_active and expired_active > 20:
            audit["critical_issues"].append(f"{expired_active} subs are marked 'active' but their current_period_end is over 7 days ago. These are likely cancelled/expired and inflate MRR.")

        # ============================================================
        # CHECK 13: Leads vs Subs email match rate
        # ============================================================
        total_leads = await conn.fetchval("SELECT COUNT(*) FROM leads WHERE email != ''")
        leads_with_sub = await conn.fetchval("""
            SELECT COUNT(DISTINCT lower(l.email)) FROM leads l
            INNER JOIN subscriptions s ON lower(l.email) = lower(s.email)
            WHERE l.email != '' AND s.email != ''
        """)
        subs_with_lead = await conn.fetchval("""
            SELECT COUNT(DISTINCT lower(s.email)) FROM subscriptions s
            INNER JOIN leads l ON lower(s.email) = lower(l.email)
            WHERE s.email != '' AND l.email != '' AND s.status IN ('active', 'trialing')
        """)
        active_with_email = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status IN ('active', 'trialing') AND email != '' AND email IS NOT NULL")
        audit["checks"]["lead_attribution"] = {
            "total_leads": total_leads or 0,
            "leads_who_subscribed": leads_with_sub or 0,
            "active_subs_with_matching_lead": subs_with_lead or 0,
            "active_subs_with_email": active_with_email or 0,
            "attribution_rate": round(((subs_with_lead or 0) / max(active_with_email or 1, 1)) * 100, 1)
        }

        # ============================================================
        # CHECK 14: Cancelled subs with NO canceled_at date
        # ============================================================
        cancel_no_date = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status = 'canceled' AND canceled_at IS NULL")
        audit["checks"]["cancel_date_quality"] = {
            "canceled_without_date": cancel_no_date or 0,
            "note": "Missing canceled_at means churn timing is unknown. Affects churn rate calculations."
        }

        # ============================================================
        # CHECK 15: Net MRR risk score
        # ============================================================
        risk_factors = []
        risk_score = 0
        if (expired_active or 0) > 20:
            risk_factors.append(f"~{expired_active} expired-but-active subs inflating MRR")
            risk_score += 3
        if len(dup_active) > 10:
            risk_factors.append(f"~{len(dup_active)} duplicate active emails inflating MRR")
            risk_score += 3
        if no_email_active > 100:
            risk_factors.append(f"~{no_email_active} active subs with no email (unverifiable)")
            risk_score += 2
        if (stale_trials or 0) > 10:
            risk_factors.append(f"~{stale_trials} stale trialing subs (trial ended)")
            risk_score += 1
        if (very_old or 0) > 50:
            risk_factors.append(f"~{very_old} active subs over 2 years old (possible zombies)")
            risk_score += 2
        if zero_amount_active > 20:
            risk_factors.append(f"~{zero_amount_active} active subs with $0 plan amount")
            risk_score += 1

        if risk_score >= 6:
            confidence = "LOW"
        elif risk_score >= 3:
            confidence = "MEDIUM"
        else:
            confidence = "HIGH"

        audit["summary"] = {
            "mrr_confidence": confidence,
            "risk_score": risk_score,
            "risk_factors": risk_factors,
            "total_critical_issues": len(audit["critical_issues"]),
            "total_warnings": len(audit["warnings"])
        }

    return audit


# --- Session 16: Database Health Check ---

@app.get("/api/admin/db-check")
async def db_check(request: Request):
    """Verify all tables, columns, and indexes exist after startup refactor."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    checks = {"tables": {}, "indexes": {}, "columns": {}, "all_ok": True}

    async with db_pool.acquire() as conn:
        expected_tables = ["leads", "page_views", "chat_sessions", "subscriptions",
                          "subscription_events", "ad_spend", "platform_metrics", "ymove_webhook_log"]
        for t in expected_tables:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", t
            )
            checks["tables"][t] = exists
            if not exists:
                checks["all_ok"] = False

        critical_cols = {
            "subscriptions": ["converted_at", "readable_id", "renewal_count", "last_renewed_at", "source", "import_batch"],
            "leads": ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "ym_source"],
            "subscription_events": ["source"],
        }
        for table, cols in critical_cols.items():
            checks["columns"][table] = {}
            for col in cols:
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2)",
                    table, col
                )
                checks["columns"][table][col] = exists
                if not exists:
                    checks["all_ok"] = False

        expected_indexes = ["idx_leads_email_lower", "idx_subs_email_lower", "idx_subs_trial_start", "idx_subs_import_batch"]
        for idx in expected_indexes:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", idx
            )
            checks["indexes"][idx] = exists
            if not exists:
                checks["all_ok"] = False

        checks["row_counts"] = {}
        for t in expected_tables:
            if checks["tables"].get(t):
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {t}")
                checks["row_counts"][t] = count

    return checks


# --- Session 16: Safe Excel/CSV Import with Batch Tracking ---

@app.post("/api/admin/import-preview")
async def import_preview(request: Request, file: UploadFile = File(...)):
    """Dry-run import: reads Excel/CSV, shows what would be imported vs skipped."""
    import csv as csv_mod
    import io as io_mod
    import hashlib

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    contents = await file.read()
    filename = file.filename or ""

    rows_to_check = []
    if filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls"):
        from openpyxl import load_workbook
        from io import BytesIO
        wb = load_workbook(BytesIO(contents), read_only=True, data_only=True)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            headers = []
            for ri, row in enumerate(ws.iter_rows(values_only=True)):
                if ri == 0:
                    headers = [str(h or "").strip().lower() for h in row]
                    continue
                if not any(row):
                    continue
                row_dict = {}
                for ci, h in enumerate(headers):
                    row_dict[h] = str(row[ci] or "").strip() if ci < len(row) else ""
                row_dict["_sheet"] = sheet_name
                rows_to_check.append(row_dict)
        wb.close()
    else:
        text = contents.decode("utf-8-sig")
        reader = csv_mod.DictReader(io_mod.StringIO(text))
        for row in reader:
            normalized = {k.strip().lower(): (v or "").strip() for k, v in row.items()}
            normalized["_sheet"] = "csv"
            rows_to_check.append(normalized)

    def get_email(row):
        for key in ["email", "e-mail", "email address"]:
            if key in row and row[key] and "@" in row[key]:
                return row[key].strip().lower()
        return ""

    def get_source(row):
        for key in ["source", "platform"]:
            if key in row:
                val = row[key].strip().lower()
                if val in ("apple", "google", "stripe"):
                    return val
        return "unknown"

    async with db_pool.acquire() as conn:
        existing_emails = await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != ''"
        )
        existing_sub_ids = await conn.fetch("SELECT stripe_subscription_id FROM subscriptions")
        total_before = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

    existing_email_set = set(r["em"] for r in existing_emails)
    existing_id_set = set(r["stripe_subscription_id"] for r in existing_sub_ids)

    would_import = []
    would_skip_duplicate = []
    would_skip_no_email = []
    by_sheet = {}

    for row in rows_to_check:
        email = get_email(row)
        source = get_source(row)
        sheet = row.get("_sheet", "unknown")
        if sheet not in by_sheet:
            by_sheet[sheet] = {"total": 0, "import": 0, "skip_dup": 0, "skip_no_email": 0}
        by_sheet[sheet]["total"] += 1
        if not email:
            would_skip_no_email.append({"sheet": sheet})
            by_sheet[sheet]["skip_no_email"] += 1
            continue
        email_hash = hashlib.md5(email.encode()).hexdigest()[:16]
        syn_id = f"import_{source}_{email_hash}"
        if email in existing_email_set or syn_id in existing_id_set:
            would_skip_duplicate.append({"email": email, "source": source, "sheet": sheet})
            by_sheet[sheet]["skip_dup"] += 1
        else:
            would_import.append({"email": email, "source": source, "sheet": sheet})
            by_sheet[sheet]["import"] += 1
            existing_email_set.add(email)
            existing_id_set.add(syn_id)

    return {
        "status": "preview",
        "filename": filename,
        "total_rows_parsed": len(rows_to_check),
        "would_import": len(would_import),
        "would_skip_duplicate": len(would_skip_duplicate),
        "would_skip_no_email": len(would_skip_no_email),
        "current_db_subscriptions": total_before,
        "after_import_estimate": total_before + len(would_import),
        "by_sheet": by_sheet,
        "sample_imports": would_import[:20],
        "sample_duplicates": would_skip_duplicate[:20],
    }


@app.post("/api/admin/import-batch")
async def import_batch(request: Request, file: UploadFile = File(...)):
    """Import subscribers with batch ID for safe revert. Use /api/admin/revert-batch to undo."""
    import csv as csv_mod
    import io as io_mod
    import hashlib

    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    contents = await file.read()
    filename = file.filename or ""
    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    rows_to_import = []
    if filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls"):
        from openpyxl import load_workbook
        from io import BytesIO
        wb = load_workbook(BytesIO(contents), read_only=True, data_only=True)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            headers = []
            for ri, row in enumerate(ws.iter_rows(values_only=True)):
                if ri == 0:
                    headers = [str(h or "").strip().lower() for h in row]
                    continue
                if not any(row):
                    continue
                row_dict = {}
                for ci, h in enumerate(headers):
                    row_dict[h] = str(row[ci] or "").strip() if ci < len(row) else ""
                rows_to_import.append(row_dict)
        wb.close()
    else:
        text = contents.decode("utf-8-sig")
        reader = csv_mod.DictReader(io_mod.StringIO(text))
        for row in reader:
            normalized = {k.strip().lower(): (v or "").strip() for k, v in row.items()}
            rows_to_import.append(normalized)

    def get_email(row):
        for key in ["email", "e-mail", "email address"]:
            if key in row and row[key] and "@" in row[key]:
                return row[key].strip().lower()
        return ""

    def get_source(row):
        for key in ["source", "platform"]:
            if key in row:
                val = row[key].strip().lower()
                if val in ("apple", "google", "stripe"):
                    return val
        return "apple"

    def get_date(row):
        for key in ["date", "sign up date", "signup_date", "signup date", "created_at"]:
            if key in row and row[key]:
                val = row[key].strip()
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        return datetime.strptime(val[:19], fmt).replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
        return None

    async with db_pool.acquire() as conn:
        existing_emails = await conn.fetch(
            "SELECT DISTINCT lower(email) as em FROM subscriptions WHERE email != ''"
        )
        existing_ids = await conn.fetch("SELECT stripe_subscription_id FROM subscriptions")
        existing_email_set = set(r["em"] for r in existing_emails)
        existing_id_set = set(r["stripe_subscription_id"] for r in existing_ids)
        count_before = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

        imported = 0
        skipped = 0
        errors = 0

        for row in rows_to_import:
            email = get_email(row)
            if not email:
                skipped += 1
                continue
            source = get_source(row)
            email_hash = hashlib.md5(email.encode()).hexdigest()[:16]
            syn_id = f"import_{source}_{email_hash}"
            if email in existing_email_set or syn_id in existing_id_set:
                skipped += 1
                continue
            period_start = get_date(row)
            plan_amount = 1999
            plan_interval = "month"
            try:
                await conn.execute("""
                    INSERT INTO subscriptions (
                        stripe_customer_id, stripe_subscription_id, email, status,
                        plan_interval, plan_amount, currency, source,
                        current_period_start, created_at, updated_at, import_batch
                    ) VALUES ('', $1, $2, 'active', $3, $4, 'usd', $5, $6, COALESCE($7, NOW()), NOW(), $8)
                    ON CONFLICT (stripe_subscription_id) DO NOTHING
                """,
                    syn_id, email, plan_interval, plan_amount, source,
                    period_start, period_start, batch_id
                )
                imported += 1
                existing_email_set.add(email)
                existing_id_set.add(syn_id)
            except Exception as e:
                print(f"[Import] Error: {e}")
                errors += 1

        count_after = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

    return {
        "status": "ok",
        "batch_id": batch_id,
        "imported": imported,
        "skipped": skipped,
        "errors": errors,
        "count_before": count_before,
        "count_after": count_after,
    }


@app.post("/api/admin/revert-batch")
async def revert_batch(request: Request):
    """Revert a batch import by deleting all records with matching import_batch tag."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    body = await request.json()
    batch_id = (body.get("batch_id") or "").strip()
    if not batch_id:
        raise HTTPException(status_code=400, detail="batch_id required")

    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE import_batch = $1", batch_id
        )
        if count == 0:
            return {"status": "ok", "deleted": 0, "message": f"No records found with batch_id '{batch_id}'"}

        confirm = body.get("confirm", False)
        if not confirm:
            sample = await conn.fetch(
                "SELECT email, source, created_at FROM subscriptions WHERE import_batch = $1 LIMIT 10",
                batch_id
            )
            return {
                "status": "confirmation_required",
                "batch_id": batch_id,
                "records_to_delete": count,
                "sample": [{"email": r["email"], "source": r["source"], "created_at": str(r["created_at"])} for r in sample],
                "message": f"This will delete {count} records. Send again with confirm: true to proceed."
            }

        result = await conn.execute(
            "DELETE FROM subscriptions WHERE import_batch = $1", batch_id
        )
        deleted = int(result.split(" ")[-1]) if result else 0

    return {"status": "ok", "batch_id": batch_id, "deleted": deleted}


@app.get("/api/admin/list-batches")
async def list_batches(request: Request):
    """List all import batches for the revert UI."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database connected")

    async with db_pool.acquire() as conn:
        batches = await conn.fetch("""
            SELECT import_batch, COUNT(*) as record_count,
                   MIN(created_at) as earliest, MAX(created_at) as latest
            FROM subscriptions
            WHERE import_batch IS NOT NULL
            GROUP BY import_batch
            ORDER BY MIN(created_at) DESC
        """)

    return {"batches": [
        {"batch_id": r["import_batch"], "records": r["record_count"],
         "earliest": str(r["earliest"]), "latest": str(r["latest"])}
        for r in batches
    ]}


# --- Apple App Store Connect Integration (Session 13) ---

import jwt as pyjwt

def generate_apple_jwt() -> str:
    """Generate a short-lived JWT for App Store Connect API."""
    if not all([APPLE_KEY_ID, APPLE_ISSUER_ID, APPLE_KEY_CONTENT]):
        raise ValueError("Apple API credentials not configured (APPLE_KEY_ID, APPLE_ISSUER_ID, APPLE_KEY_CONTENT)")
    import time as _time
    now = int(_time.time())
    payload = {
        "iss": APPLE_ISSUER_ID,
        "iat": now,
        "exp": now + (20 * 60),
        "aud": "appstoreconnect-v1",
    }
    headers = {"alg": "ES256", "kid": APPLE_KEY_ID, "typ": "JWT"}
    return pyjwt.encode(payload, APPLE_KEY_CONTENT, algorithm="ES256", headers=headers)


async def apple_api_get(path: str, params: dict = None, expect_binary: bool = False):
    """Authenticated GET to App Store Connect API."""
    token = generate_apple_jwt()
    url = f"https://api.appstoreconnect.apple.com{path}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, params=params or {})
        print(f"[Apple API] GET {path} -> {resp.status_code}")
        if expect_binary and resp.status_code == 200:
            return resp.content
        if resp.status_code != 200:
            try:
                error_data = resp.json()
            except Exception:
                error_data = {"raw": resp.text[:500]}
            print(f"[Apple API] Error: {json.dumps(error_data, indent=2)[:500]}")
            return {"error": True, "status": resp.status_code, "data": error_data}
        return resp.json()


def parse_apple_tsv(gzipped_content: bytes) -> list:
    """Decompress gzipped TSV from Apple and return list of row dicts."""
    try:
        decompressed = gzip.decompress(gzipped_content)
        text = decompressed.decode("utf-8").strip()
    except Exception:
        text = gzipped_content.decode("utf-8").strip()
    lines = text.split("\n")
    if not lines:
        return []
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split("\t")
        row = {}
        for i, h in enumerate(headers):
            row[h.strip()] = values[i].strip() if i < len(values) else ""
        rows.append(row)
    return rows


def aggregate_apple_subscription_rows(rows: list) -> dict:
    """Sum active subscriptions and free trials across all products/countries."""
    total_active = 0
    total_trials = 0
    for row in rows:
        try:
            total_active += int(row.get("Active Subscriptions",
                row.get("Active Standard Price Subscriptions", "0")) or 0)
            total_trials += int(row.get("Active Free Trial Introductory Offer Subscriptions",
                row.get("Active Free Trials", "0")) or 0)
        except (ValueError, TypeError):
            pass
    return {"active_subscriptions": total_active, "active_free_trials": total_trials}


def aggregate_apple_event_rows(rows: list) -> dict:
    """Categorize subscription events and sum revenue."""
    new_subs = renewals = conversions = cancellations = reactivations = 0
    revenue_cents = proceeds_cents = 0
    for row in rows:
        event = row.get("Event", "").lower()
        try:
            units = int(row.get("Quantity", row.get("Units", "0")) or 0)
        except (ValueError, TypeError):
            units = 0
        try:
            dev_proceeds = float(row.get("Developer Proceeds", row.get("Proceeds", "0")) or 0)
            cust_price = float(row.get("Customer Price", "0") or 0)
        except (ValueError, TypeError):
            dev_proceeds = cust_price = 0
        if any(x in event for x in ["new", "subscribe", "initial"]):
            new_subs += units
        if "renew" in event:
            renewals += units
        if any(x in event for x in ["convert", "paid from"]):
            conversions += units
        if any(x in event for x in ["cancel", "churn", "refund"]):
            cancellations += units
        if "reactivat" in event:
            reactivations += units
        revenue_cents += round(cust_price * 100 * units) if cust_price else 0
        proceeds_cents += round(dev_proceeds * 100 * units) if dev_proceeds else 0
    return {
        "new_subscriptions": new_subs, "renewals": renewals,
        "conversions": conversions, "cancellations": cancellations,
        "reactivations": reactivations,
        "revenue_cents": revenue_cents, "proceeds_cents": proceeds_cents,
    }


async def store_apple_subscription_metric(conn, report_date, totals: dict, raw_rows: list):
    """Upsert subscription snapshot into platform_metrics."""
    await conn.execute("""
        INSERT INTO platform_metrics (date, source, metric_type,
            active_subscriptions, active_free_trials, report_data, updated_at)
        VALUES ($1, 'apple', 'subscription', $2, $3, $4, NOW())
        ON CONFLICT (date, source, metric_type) DO UPDATE SET
            active_subscriptions = EXCLUDED.active_subscriptions,
            active_free_trials = EXCLUDED.active_free_trials,
            report_data = EXCLUDED.report_data,
            updated_at = NOW()
    """, report_date, totals["active_subscriptions"], totals["active_free_trials"],
        json.dumps(raw_rows[:50]))


async def store_apple_event_metric(conn, report_date, totals: dict, raw_rows: list):
    """Upsert event data into platform_metrics."""
    await conn.execute("""
        INSERT INTO platform_metrics (date, source, metric_type,
            new_subscriptions, renewals, conversions, cancellations,
            reactivations, revenue_cents, proceeds_cents, report_data, updated_at)
        VALUES ($1, 'apple', 'subscription_event', $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (date, source, metric_type) DO UPDATE SET
            new_subscriptions = EXCLUDED.new_subscriptions,
            renewals = EXCLUDED.renewals,
            conversions = EXCLUDED.conversions,
            cancellations = EXCLUDED.cancellations,
            reactivations = EXCLUDED.reactivations,
            revenue_cents = EXCLUDED.revenue_cents,
            proceeds_cents = EXCLUDED.proceeds_cents,
            report_data = EXCLUDED.report_data,
            updated_at = NOW()
    """, report_date, totals["new_subscriptions"], totals["renewals"],
        totals["conversions"], totals["cancellations"], totals["reactivations"],
        totals["revenue_cents"], totals["proceeds_cents"],
        json.dumps(raw_rows[:50]))


@app.get("/api/admin/apple-discover")
async def apple_discover(request: Request):
    """Discover Apple app details: ID, bundle, subscription groups, test vendor."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    result = {"steps": []}

    # Step 1: List apps
    apps_resp = await apple_api_get("/v1/apps")
    if isinstance(apps_resp, dict) and apps_resp.get("error"):
        return {"error": "Failed to list apps", "detail": apps_resp}

    apps = apps_resp.get("data", [])
    result["apps"] = []
    for a in apps:
        attrs = a.get("attributes", {})
        app_info = {
            "id": a.get("id"),
            "name": attrs.get("name"),
            "bundleId": attrs.get("bundleId"),
            "sku": attrs.get("sku"),
        }
        result["apps"].append(app_info)

        # Step 2: Get subscription groups
        groups_resp = await apple_api_get(f"/v1/apps/{a['id']}/subscriptionGroups")
        if isinstance(groups_resp, dict) and not groups_resp.get("error"):
            groups = groups_resp.get("data", [])
            app_info["subscription_groups"] = []
            for g in groups:
                group_info = {
                    "id": g.get("id"),
                    "name": g.get("attributes", {}).get("referenceName"),
                }
                subs_resp = await apple_api_get(f"/v1/subscriptionGroups/{g['id']}/subscriptions")
                if isinstance(subs_resp, dict) and not subs_resp.get("error"):
                    group_info["subscriptions"] = [
                        {"id": s.get("id"), "name": s.get("attributes", {}).get("name"),
                         "productId": s.get("attributes", {}).get("productId"),
                         "state": s.get("attributes", {}).get("state")}
                        for s in subs_resp.get("data", [])
                    ]
                app_info["subscription_groups"].append(group_info)

    result["steps"].append("Listed apps and subscription groups")

    # Step 3: Test vendor number if configured
    if APPLE_VENDOR_NUMBER:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        test_resp = await apple_api_get("/v1/salesReports", params={
            "filter[reportType]": "SALES",
            "filter[reportSubType]": "SUMMARY",
            "filter[frequency]": "DAILY",
            "filter[reportDate]": yesterday,
            "filter[vendorNumber]": APPLE_VENDOR_NUMBER,
        }, expect_binary=True)
        if isinstance(test_resp, dict) and test_resp.get("error"):
            result["vendor_test"] = {"status": "failed", "detail": test_resp}
        else:
            result["vendor_test"] = {"status": "success", "vendor": APPLE_VENDOR_NUMBER, "report_size_bytes": len(test_resp)}
        result["steps"].append(f"Tested vendor number: {APPLE_VENDOR_NUMBER}")
    else:
        result["vendor_number_help"] = "NOT SET. Set APPLE_VENDOR_NUMBER env var. Find it in App Store Connect > Payments and Financial Reports > top left gray bar."
        result["steps"].append("No vendor number configured")

    return result


@app.post("/api/admin/apple-pull-report")
async def apple_pull_report(request: Request):
    """Pull Apple subscription reports for a date and store in platform_metrics.
    Body: {"date": "2026-03-10"} â defaults to 2 days ago (Apple ~2 day lag)."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not APPLE_VENDOR_NUMBER:
        raise HTTPException(status_code=400, detail="APPLE_VENDOR_NUMBER not configured. Run /api/admin/apple-discover first.")

    body = await request.json()
    report_date_str = body.get("date", "")
    if not report_date_str:
        report_date_str = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

    report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
    results = {"date": report_date_str, "reports": {}}

    # --- SUBSCRIPTION report (active counts snapshot) ---
    print(f"[Apple] Pulling SUBSCRIPTION report for {report_date_str}")
    sub_resp = await apple_api_get("/v1/salesReports", params={
        "filter[reportType]": "SUBSCRIPTION",
        "filter[reportSubType]": "SUMMARY",
        "filter[frequency]": "DAILY",
        "filter[reportDate]": report_date_str,
        "filter[vendorNumber]": APPLE_VENDOR_NUMBER,
    }, expect_binary=True)

    if isinstance(sub_resp, dict) and sub_resp.get("error"):
        results["reports"]["subscription"] = {"error": sub_resp}
    elif isinstance(sub_resp, bytes):
        rows = parse_apple_tsv(sub_resp)
        totals = aggregate_apple_subscription_rows(rows)
        results["reports"]["subscription"] = {"rows": len(rows), "totals": totals, "sample": rows[:3]}
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await store_apple_subscription_metric(conn, report_date, totals, rows)
                results["reports"]["subscription"]["stored"] = True
            except Exception as e:
                results["reports"]["subscription"]["store_error"] = str(e)

    # --- SUBSCRIPTION_EVENT report (daily activity) ---
    print(f"[Apple] Pulling SUBSCRIPTION_EVENT report for {report_date_str}")
    event_resp = await apple_api_get("/v1/salesReports", params={
        "filter[reportType]": "SUBSCRIPTION_EVENT",
        "filter[reportSubType]": "SUMMARY",
        "filter[frequency]": "DAILY",
        "filter[reportDate]": report_date_str,
        "filter[vendorNumber]": APPLE_VENDOR_NUMBER,
    }, expect_binary=True)

    if isinstance(event_resp, dict) and event_resp.get("error"):
        results["reports"]["subscription_event"] = {"error": event_resp}
    elif isinstance(event_resp, bytes):
        rows = parse_apple_tsv(event_resp)
        totals = aggregate_apple_event_rows(rows)
        results["reports"]["subscription_event"] = {"rows": len(rows), "totals": totals, "sample": rows[:5]}
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await store_apple_event_metric(conn, report_date, totals, rows)
                results["reports"]["subscription_event"]["stored"] = True
            except Exception as e:
                results["reports"]["subscription_event"]["store_error"] = str(e)

    return results


@app.post("/api/admin/apple-backfill")
async def apple_backfill(request: Request):
    """Pull Apple reports for a date range. Max 90 days per run.
    Body: {"start_date": "2026-01-01", "end_date": "2026-03-10"}"""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not APPLE_VENDOR_NUMBER:
        raise HTTPException(status_code=400, detail="APPLE_VENDOR_NUMBER not configured")

    body = await request.json()
    start = body.get("start_date", "")
    end = body.get("end_date", "")
    if not start or not end:
        raise HTTPException(status_code=400, detail="start_date and end_date required (YYYY-MM-DD)")

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    if (end_dt - start_dt).days > 90:
        raise HTTPException(status_code=400, detail="Max 90 days per backfill run")

    results = {"days_processed": 0, "errors": [], "success": []}
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        rd = current.date()

        for report_type, metric_type in [("SUBSCRIPTION", "subscription"), ("SUBSCRIPTION_EVENT", "subscription_event")]:
            try:
                resp = await apple_api_get("/v1/salesReports", params={
                    "filter[reportType]": report_type,
                    "filter[reportSubType]": "SUMMARY",
                    "filter[frequency]": "DAILY",
                    "filter[reportDate]": date_str,
                    "filter[vendorNumber]": APPLE_VENDOR_NUMBER,
                }, expect_binary=True)

                if isinstance(resp, dict) and resp.get("error"):
                    status = resp.get("status", 0)
                    if status != 404:
                        results["errors"].append(f"{date_str} {report_type}: {status}")
                elif isinstance(resp, bytes) and db_pool:
                    rows = parse_apple_tsv(resp)
                    async with db_pool.acquire() as conn:
                        if metric_type == "subscription":
                            totals = aggregate_apple_subscription_rows(rows)
                            await store_apple_subscription_metric(conn, rd, totals, rows)
                        else:
                            totals = aggregate_apple_event_rows(rows)
                            await store_apple_event_metric(conn, rd, totals, rows)
            except Exception as e:
                results["errors"].append(f"{date_str} {report_type}: {str(e)}")

        results["days_processed"] += 1
        results["success"].append(date_str)
        current += timedelta(days=1)

    return results


@app.get("/api/admin/apple-metrics")
async def apple_metrics(request: Request):
    """Get aggregated Apple metrics for dashboard. Optional date range."""
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)
    if not db_pool:
        raise HTTPException(status_code=500, detail="No database")

    date_from = request.query_params.get("from", "")
    date_to = request.query_params.get("to", "")

    async with db_pool.acquire() as conn:
        # Latest subscription snapshot
        latest_sub = await conn.fetchrow("""
            SELECT * FROM platform_metrics
            WHERE source = 'apple' AND metric_type = 'subscription'
            ORDER BY date DESC LIMIT 1
        """)

        # Event totals for date range or last 30 days
        if date_from and date_to:
            events = await conn.fetch("""
                SELECT * FROM platform_metrics
                WHERE source = 'apple' AND metric_type = 'subscription_event'
                AND date >= $1 AND date <= $2 ORDER BY date
            """, datetime.strptime(date_from, "%Y-%m-%d").date(),
                datetime.strptime(date_to, "%Y-%m-%d").date())
        else:
            events = await conn.fetch("""
                SELECT * FROM platform_metrics
                WHERE source = 'apple' AND metric_type = 'subscription_event'
                AND date > CURRENT_DATE - INTERVAL '30 days' ORDER BY date
            """)

        total_new = total_renewals = total_conversions = total_cancellations = 0
        total_revenue = total_proceeds = 0
        daily_data = []
        for e in events:
            total_new += e["new_subscriptions"] or 0
            total_renewals += e["renewals"] or 0
            total_conversions += e["conversions"] or 0
            total_cancellations += e["cancellations"] or 0
            total_revenue += e["revenue_cents"] or 0
            total_proceeds += e["proceeds_cents"] or 0
            daily_data.append({
                "date": str(e["date"]),
                "new_subs": e["new_subscriptions"] or 0,
                "renewals": e["renewals"] or 0,
                "conversions": e["conversions"] or 0,
                "cancellations": e["cancellations"] or 0,
                "revenue_cents": e["revenue_cents"] or 0,
                "proceeds_cents": e["proceeds_cents"] or 0,
            })

    return {
        "current_snapshot": {
            "date": str(latest_sub["date"]) if latest_sub else None,
            "active_subscriptions": latest_sub["active_subscriptions"] if latest_sub else 0,
            "active_free_trials": latest_sub["active_free_trials"] if latest_sub else 0,
        } if latest_sub else None,
        "period_totals": {
            "new_subscriptions": total_new,
            "renewals": total_renewals,
            "conversions": total_conversions,
            "cancellations": total_cancellations,
            "revenue_cents": total_revenue,
            "revenue_display": f"${total_revenue / 100:,.2f}",
            "proceeds_cents": total_proceeds,
            "proceeds_display": f"${total_proceeds / 100:,.2f}",
            "apple_fee_cents": total_revenue - total_proceeds,
        },
        "daily": daily_data,
        "days_with_data": len(daily_data),
    }



# --- Static + Routing ---

@app.get("/mm-admin")
async def admin_page():
    return FileResponse("static/admin.html")

@app.get("/")
async def root():
    return FileResponse("static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/{path:path}")
async def catch_all(path: str):
    return FileResponse("static/index.html")
