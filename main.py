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
                # Phase 5: UTM tracking columns on leads
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_source TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_medium TEXT DEFAULT ''")
                await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_campaign TEXT DEFAULT ''")

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
    utm_source: str = ""
    utm_medium: str = ""
    utm_campaign: str = ""

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
                    """INSERT INTO leads (first_name, email, experience_level, goals, referral_source, recommended_plan, extra, utm_source, utm_medium, utm_campaign)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
                    lead.first_name, lead.email, lead.experience_level,
                    lead.goals, lead.referral_source, lead.recommended_plan, lead.extra,
                    lead.utm_source, lead.utm_medium, lead.utm_campaign
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

    return {"status": "ok"}


# --- Admin Endpoints ---

@app.post("/api/admin/login")
async def admin_login(req: LoginRequest):
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"status": "ok"}


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
                customer_id = sub.customer.id if hasattr(sub.customer, "id") else str(sub.customer)
                status = sub.status

                plan_amount = 0
                plan_interval = ""
                if sub.items and sub.items.data:
                    price = sub.items.data[0].price
                    plan_amount = price.unit_amount or 0
                    if price.recurring:
                        plan_interval = price.recurring.interval or ""

                email = ""
                try:
                    if hasattr(sub.customer, "email"):
                        email = sub.customer.email or ""
                except Exception:
                    pass

                def ts(v):
                    if v:
                        return datetime.fromtimestamp(v, tz=timezone.utc)
                    return None

                async with db_pool.acquire() as conn:
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
                        plan_interval, plan_amount, sub.currency or "usd",
                        ts(sub.trial_start), ts(sub.trial_end),
                        ts(sub.current_period_start), ts(sub.current_period_end),
                        ts(sub.canceled_at)
                    )
                count += 1
            except Exception as e:
                print(f"Backfill sub error: {e}")
                errors += 1
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe API error: {str(e)}")

    return {"status": "ok", "imported": count, "errors": errors}


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
        await conn.execute("TRUNCATE leads, page_views, chat_sessions, subscriptions, subscription_events RESTART IDENTITY")

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

@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    pw = request.headers.get("X-Admin-Password", "")
    require_admin(pw)

    if not db_pool:
        return {"error": "No database connected"}

    # Date range filtering (optional query params)
    from datetime import timedelta
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

        # Avg subscription age (days)
        avg_sub_age = await conn.fetchval(
            """SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - created_at)) / 86400), 0)
               FROM subscriptions WHERE status IN ('active', 'trialing')"""
        )

        # Estimated LTV = avg monthly revenue per sub * avg lifetime months
        avg_monthly_per_sub = await conn.fetchval(
            """SELECT COALESCE(AVG(
                CASE WHEN plan_interval='month' THEN plan_amount
                     WHEN plan_interval='year' THEN plan_amount/12
                     ELSE 0 END
               ), 0) FROM subscriptions WHERE status = 'active'"""
        )

        # Total subscribers ever (for lifetime calc)
        total_subs_ever = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")

        # ARR
        arr_cents = mrr_cents * 12

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
    writer.writerow(["id", "first_name", "email", "experience_level", "goals", "referral_source", "recommended_plan", "extra", "utm_source", "utm_medium", "utm_campaign", "created_at"])
    for r in rows:
        writer.writerow([r["id"], r["first_name"], r["email"], r["experience_level"], r["goals"], r["referral_source"], r["recommended_plan"], r["extra"], r.get("utm_source",""), r.get("utm_medium",""), r.get("utm_campaign",""), str(r["created_at"])])

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
        "version": "9.0.0",
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
