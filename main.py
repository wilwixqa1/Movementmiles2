from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import httpx
import os
import json
import asyncpg
import stripe
from datetime import datetime, timezone

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

stripe.api_key = STRIPE_SECRET_KEY

# --- Database ---
db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
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
                        trial_start TIMESTAMPTZ,
                        trial_end TIMESTAMPTZ,
                        current_period_start TIMESTAMPTZ,
                        current_period_end TIMESTAMPTZ,
                        canceled_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscription_events (
                        id SERIAL PRIMARY KEY,
                        stripe_event_id TEXT UNIQUE,
                        event_type TEXT,
                        stripe_customer_id TEXT,
                        stripe_subscription_id TEXT,
                        data JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            print("Database connected, all tables ready")
        except Exception as e:
            print(f"Database connection failed: {e}")
            db_pool = None
    else:
        print("No DATABASE_URL — running without database")

@app.on_event("shutdown")
async def shutdown():
    global db_pool
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


ONBOARD_SYSTEM_PROMPT = """You are Nelly, the AI coaching assistant for Movement & Miles. You're greeting someone on the get-started page and your job is to have a friendly onboarding conversation.

PERSONALITY: Warm, encouraging, casual. Like a friend who coaches on the side.

YOUR GOAL: Collect their info through natural conversation, then recommend a plan. Collect these ONE AT A TIME:
1. First name
2. Email address
3. Primary goals (running, strength, race training, weight loss, injury recovery)
4. Experience level (beginner, intermediate, advanced)
5. Equipment access (weights, treadmill)
6. How they heard about M&M

BUTTON FORMAT: End messages with options when appropriate:
[Option A | Option B | Option C]

FLOW:
- Start by asking their name
- Then email
- Then walk through goals, level, equipment naturally
- After collecting everything, recommend 3 programs with brief reasoning
- End with the LEAD tag (user won't see this)

LEAD TAG: After your final recommendation, emit this EXACT format on its own line:
[[LEAD:{"first_name":"...","email":"...","experience_level":"...","goals":"...","referral_source":"...","recommended_plan":"..."}]]

The lead tag must be valid JSON inside the [[LEAD:...]] wrapper. Include all fields even if empty string.

Keep responses SHORT (2-3 sentences). One question at a time. Be conversational, not robotic."""


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


def require_admin(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


# --- Pydantic Models ---

class ChatRequest(BaseModel):
    message: str
    history: list = []

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

class PageViewRequest(BaseModel):
    page: str = ""
    path: str = ""
    referrer: str = ""

class LoginRequest(BaseModel):
    password: str


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
                    "widget", 1
                )
        except Exception:
            pass
    return ChatResponse(reply=reply)


@app.post("/api/onboard-chat", response_model=ChatResponse)
async def onboard_chat(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    messages = []
    for msg in req.history[-20:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})
    reply = await call_anthropic(ONBOARD_SYSTEM_PROMPT, messages)
    # Track chat session
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO chat_sessions (session_type, message_count) VALUES ($1, $2)",
                    "onboard", 1
                )
        except Exception:
            pass
    return ChatResponse(reply=reply)


# Lead capture
@app.post("/api/lead")
async def save_lead(lead: LeadRequest):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO leads (first_name, email, experience_level, goals, referral_source, recommended_plan, extra)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                    lead.first_name, lead.email, lead.experience_level,
                    lead.goals, lead.referral_source, lead.recommended_plan, lead.extra
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
                        plan_interval, plan_amount, currency,
                        trial_start, trial_end,
                        current_period_start, current_period_end,
                        canceled_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
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

        # Handle one-time payments (checkout.session.completed for initial signup)
        elif event_type == "checkout.session.completed":
            session = data_obj
            # If this checkout created a subscription, it'll be handled by subscription.created
            # Log it for analytics
            print(f"Checkout completed: {session.get('id')}, customer: {session.get('customer')}")

    return {"status": "ok"}


# --- Admin Endpoints ---

@app.post("/api/admin/login")
async def admin_login(req: LoginRequest):
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"status": "ok"}


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not db_pool:
        return {"error": "No database connected"}

    async with db_pool.acquire() as conn:
        # Leads
        total_leads = await conn.fetchval("SELECT COUNT(*) FROM leads")
        recent_leads = await conn.fetch(
            "SELECT * FROM leads ORDER BY created_at DESC LIMIT 50"
        )

        # Page views
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

        # Trial → paid conversion
        total_ever_trialed = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE trial_start IS NOT NULL"
        )
        converted_from_trial = await conn.fetchval(
            """SELECT COUNT(*) FROM subscriptions
               WHERE trial_start IS NOT NULL AND status = 'active'"""
        )

        # Churn: canceled in last 30 days vs active at start of period
        churned_30d = await conn.fetchval(
            """SELECT COUNT(*) FROM subscriptions
               WHERE status = 'canceled' AND canceled_at > NOW() - INTERVAL '30 days'"""
        )

        # Recent subscription events
        recent_events = await conn.fetch(
            """SELECT event_type, stripe_customer_id, created_at
               FROM subscription_events ORDER BY created_at DESC LIMIT 20"""
        )

        # Subscriptions by status
        subs_by_status = await conn.fetch(
            "SELECT status, COUNT(*) as count FROM subscriptions GROUP BY status ORDER BY count DESC"
        )

    # Build response
    trial_conversion_rate = 0
    if total_ever_trialed and total_ever_trialed > 0:
        trial_conversion_rate = round((converted_from_trial / total_ever_trialed) * 100, 1)

    churn_rate = 0
    if active_subs and active_subs > 0:
        churn_rate = round((churned_30d / (active_subs + churned_30d)) * 100, 1)

    return {
        "leads": {
            "total": total_leads,
            "recent": [dict(r) for r in recent_leads],
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
            "churn_rate_30d": churn_rate,
            "churned_30d": churned_30d or 0,
            "by_status": [{"status": r["status"], "count": r["count"]} for r in subs_by_status],
            "recent_events": [
                {
                    "event_type": r["event_type"],
                    "customer": r["stripe_customer_id"],
                    "created_at": str(r["created_at"]),
                }
                for r in recent_events
            ],
        },
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
    writer.writerow(["id", "first_name", "email", "experience_level", "goals", "referral_source", "recommended_plan", "extra", "created_at"])
    for r in rows:
        writer.writerow([r["id"], r["first_name"], r["email"], r["experience_level"], r["goals"], r["referral_source"], r["recommended_plan"], r["extra"], str(r["created_at"])])

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
    return {
        "status": "ok",
        "service": "Movement & Miles",
        "version": "7.0",
        "database": db_status,
        "stripe": stripe_status,
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
