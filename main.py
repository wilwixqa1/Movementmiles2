from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
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
            async with db_pool.acquire() as conn:
                # Phase 2 tables
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
                # Phase 5: UTM tracking columns on leads
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_source TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_medium TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_campaign TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_term TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_content TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS ym_source TEXT DEFAULT ''")

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS chat_sessions (
                        id SERIAL PRIMARY KEY,
                        session_type TEXT,
                        message_count INTEGER DEFAULT 1,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                # Phase 3 tables
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
                # Phase 4: add source column if table already existed without it
                await conn.execute("""
                    ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'stripe'
                """)
                # Session 11: trial-to-paid conversion tracking
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS readable_id TEXT")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS renewal_count INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS last_renewed_at TIMESTAMPTZ")

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
                # Phase 4: add source column if table already existed without it
                await conn.execute("""
                    ALTER TABLE subscription_events ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'stripe'
                """)

                # Session 11: Ad spend tracking table
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
                # Session 13: Apple/Google aggregate metrics
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
            print("Database connected, all tables ready")
        except Exception as e:
            print(f"Database connection failed: {e}")
            db_pool = None
    else:
        print("No DATABASE_URL — running without database")

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
            scheduler.start()
            print(f"Daily digest scheduler started (9:00 AM ET -> {DIGEST_RECIPIENTS})")
        except Exception as e:
            print(f"Scheduler startup error: {e}")
    else:
        missing = []
        if not RESEND_API_KEY:
            missing.append("RESEND_API_KEY")
        if not DIGEST_RECIPIENTS:
            missing.append("DIGEST_RECIPIENTS")
        print(f"Daily digest disabled — missing: {', '.join(missing)}")

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

CRITICAL CONTEXT — TRIAL vs PAID:
- "new_subscriptions_trial_starts" are TRIAL STARTS — these are $0 revenue. The checkout flow gives 1 month free, then $19.99/month.
- "conversions_today" are the real revenue events — people whose free trial ended and converted to paid.
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

async def call_anthropic(system_prompt: str, messages: list, max_tokens: int = 800) -> str:
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="API key not configured")
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_prompt,
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


async def call_anthropic_raw(system_prompt: str, messages: list, max_tokens: int = 800) -> str:
    """Like call_anthropic but doesn't raise HTTPException — returns error string instead."""
    if not ANTHROPIC_API_KEY:
        return "[Error: ANTHROPIC_API_KEY not configured]"
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_prompt,
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
    reply = await call_anthropic(NELLY_SYSTEM_PROMPT, messages)
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
        # No webhook secret configured — parse raw (dev/testing only)
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

    # Decode JWS payload (header.payload.signature — we want the middle part)
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
               ORDER BY created_at DESC"""
        )
        new_subs_by_source = await conn.fetch(
            """SELECT source, COUNT(*) as count FROM subscriptions
               WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY source ORDER BY count DESC"""
        )

        # Conversions (trial → paid) in last 24h
        conversions_today = await conn.fetch(
            """SELECT source, email, plan_interval, plan_amount, converted_at
               FROM subscriptions WHERE converted_at > NOW() - INTERVAL '24 hours'
               ORDER BY converted_at DESC"""
        )

        # Cancellations in last 24h
        cancellations = await conn.fetch(
            """SELECT source, email, canceled_at FROM subscriptions
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
        "cancel_details": [{"email": r["email"] or "n/a", "source": r["source"]} for r in cancellations],
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
        cancel_rows += f'<tr><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{c["email"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{c["source"]}</td></tr>'

    lead_source_rows = ""
    for ls in stats.get("leads_by_source", []):
        lead_source_rows += f'<tr><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{ls["source"]}</td><td style="padding:6px 12px;font-size:14px;border-bottom:1px solid #e0e0e0">{ls["count"]}</td></tr>'

    # Session 11: Convert markdown bold **text** to <strong>text</strong>
    insights_clean = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', insights)
    # Format insights — convert newlines and bullet points to HTML
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
  <h3 style="margin:0 0 8px;color:#c0392b;font-size:14px;font-weight:600">Cancellations</h3>
  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
    <tr style="background:#f7f7f7"><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Email</th><th style="padding:8px 12px;font-size:11px;text-align:left;color:#536c7c;font-weight:600;text-transform:uppercase">Source</th></tr>
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
    """Orchestrator: gather stats -> AI insights -> build email -> send."""
    print(f"[Digest] Starting daily digest at {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M ET')}")
    try:
        stats = await gather_daily_stats()
        if "error" in stats:
            print(f"[Digest] Error gathering stats: {stats['error']}")
            return

        insights = await generate_digest_insights(stats)

        now_et = datetime.now(ZoneInfo("America/New_York"))
        subject = f"M&M Daily Digest - {now_et.strftime('%b %d')} | {stats.get('conversions_today',0)} conversions, {stats.get('new_subscriptions',0)} trials, {stats.get('cancellations',0)} cancellations"

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
    subject = f"[TEST] M&M Daily Digest - {now_et.strftime('%b %d')} | {stats.get('conversions_today',0)} conversions, {stats.get('new_subscriptions',0)} trials, {stats.get('cancellations',0)} cancellations"

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

        # Avg subscriber lifetime (all who ever converted — includes churned for honest LTV)
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
    return {
        "status": "ok",
        "service": "Movement & Miles",
        "version": "14.0.0",
        "database": db_status,
        "stripe": stripe_status,
        "daily_digest": digest_status,
        "digest_recipients": DIGEST_RECIPIENTS if DIGEST_RECIPIENTS else "none",
    }



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
    Body: {"date": "2026-03-10"} — defaults to 2 days ago (Apple ~2 day lag)."""
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
